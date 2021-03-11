#!/usr/bin/env python3
import requests, subprocess, socket, sys, os, time, tempfile, json
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from pathlib import Path

LOCAL_SITE = socket.gethostname()[1:]
THIS_PROG = sys.argv[0]
WALT_SERVER_ENV_FILE = 'http://public.grenoble.grid5000.fr/~eduble/walt-server-7.env'

########### configuration ##############
WALT_NODE_COUNTS = {
    'nancy': 1,
    'grenoble': 1
}
WALT_SERVER_SITE = 'nancy'
WALLTIME = "01:00:00"

LOG_CONF = Path('walt-platform.conf')
########### end of configuration #######

def append_line(file_path, s):
    with file_path.open(mode="a") as f:
        f.write(s + '\n')

def update_nodes(env):
    for site, site_info in env['sites'].items():
        # note: if site is not local, cmd will actually be
        # $ ssh <site> oarstat <opts> | oarprint <opts>
        # thus oarstat will be run remotely whilst oarprint will be run locally.
        # (this is working fine, but worth mentionning if debugging this part.)
        info = run_cmd_on_site(env, site,
                f'oarstat -p | oarprint host -P eth_count,host -f -', True)
        walt_nodes = []
        for line in info.strip().split('\n'):
            if line.strip() == '':
                continue
            eth_count, host = line.split()
            if env['server']['site'] == site and \
               env['server'].get('host') is None and \
               int(eth_count) > 1:
                env['server']['host'] = host
            else:
                walt_nodes.append(host)
        env['sites'][site]['nodes'] = walt_nodes

def run_cmd_on_site(env, site, cmd, add_job_id_option=False, **run_kwargs):
    if add_job_id_option:
        site_job_id = env['sites'][site]['job_id']
        args = cmd.split()
        args[1:1] = [ '-j', str(site_job_id) ]
        cmd = ' '.join(args)
    cmd = cmd_on_site(site, cmd)
    info = subprocess.run(cmd, shell=True, capture_output=True,
                          encoding = sys.stdout.encoding, **run_kwargs)
    return info.stdout

def update_vlan_id(env):
    vlan_site = env['vlan']['site']
    env['vlan']['vlan_id'] = int(
        run_cmd_on_site(env, vlan_site, 'kavlan -V', True))

# build a list of g5k sites to look into when looking for a global vlan,
# preferred sites first.
def get_sorted_sites_for_global_vlan(config_per_site):
    resp = requests.get(f'https://api.grid5000.fr/sid/sites.conf')
    all_sites = set(site['uid'] for site in resp.json()['items'])
    # try local site preferably
    sorted_sites = [LOCAL_SITE]
    all_sites.remove(LOCAL_SITE)
    # then try other sites where walt nodes will be reserved
    for site in config_per_site.keys():
        if site not in sorted_sites:
            sorted_sites.append(site)
            all_sites.remove(site)
    # then try remaining sites
    sorted_sites += list(all_sites)
    return sorted_sites

def find_vlan_in_sites(requested_vlan_type, sites, allow_busy=False):
    for site in sites:
        resp = requests.get(f'https://api.grid5000.fr/sid/sites/{site}/status.conf')
        site_status = resp.json()
        for vlan_id, vlan_info in site_status['vlans'].items():
            vlan_id = int(vlan_id)
            if vlan_info['hard'] != 'alive':
                continue
            if not allow_busy and vlan_info['soft'] != 'free':
                continue
            vlan_type = vlan_info['type']
            if vlan_type == requested_vlan_type:
                continue
            return site
    return None

def add_vlan_config(env):
    site = None
    config_per_site = env['sites']
    if len(config_per_site) == 1:
        vlan_type = 'kavlan-local'
        site = find_vlan_in_sites(vlan_type, [LOCAL_SITE])
    else:
        vlan_type = 'kavlan-global'
        site = find_vlan_in_sites(vlan_type,
                get_sorted_sites_for_global_vlan(config_per_site))
    if site is None:
        # default to local site, even if this means delaying the job
        print('WARNING: No VLAN available for now. Job will not run immediately.')
        site = find_vlan_in_sites(vlan_type, [LOCAL_SITE], allow_busy=True)
    config_per_site[site][vlan_type] = True
    env['vlan'] = {
        'site': site,
        'type': vlan_type
    }

def follow(cmd):
    popen = subprocess.Popen(cmd,
            stdout = subprocess.PIPE,
            encoding = sys.stdout.encoding,
            shell = True)
    while True:
        try:
            line = popen.stdout.readline()
            if line == '':
                break   # empty read
            yield line.strip()
        except GeneratorExit:
            if popen.poll() is not None:
                popen.stdout.close()
                popen.terminate()
                popen.wait()
            raise

def submit(config_per_site):
    all_sites_resource_specs = []
    for site, site_config in config_per_site.items():
        site_resources = []
        if site_config.get('server', False):
            site_resources += ["""{"eth_count>1"}/nodes=1"""]
        num_nodes = site_config.get('num_nodes', 0)
        if num_nodes > 0:
            site_resources += [f"/nodes={num_nodes}"]
        if site_config.get('kavlan-local', False):
            site_resources += ["""{"type='kavlan-local'"}/vlan=1"""]
        if site_config.get('kavlan-global', False):
            site_resources += ["""{"type='kavlan-global'"}/vlan=1"""]
        site_resource_specs = f'{site}:rdef=' + '+'.join(site_resources)
        all_sites_resource_specs.append(site_resource_specs)
    resources = ",".join(all_sites_resource_specs)
    resources = resources.replace('"', '\\'*7 + '"')    # funny double-quote escaping
    cmd = f'oargridsub -t deploy -w {WALLTIME} "{resources}"'
    return follow(cmd)

def cmd_on_site(site, cmd, tty=False):
    if site != LOCAL_SITE:
        if tty:
            cmd = f"ssh -t {site} -- {cmd}"
        else:
            cmd = f"ssh {site} -- {cmd}"
    return cmd

def oarstat(env, site):
    job_id = env['sites'][site]['job_id']
    out = run_cmd_on_site(env, site, f"oarstat -j {job_id} -f -J")
    return json.loads(out)[job_id]

def human_readable_delay(seconds):
    if seconds < 1:
        seconds = 1
    delta = relativedelta(seconds=seconds)
    items = []
    for attr in ['years', 'months', 'days', 'hours', 'minutes', 'seconds']:
        attr_val = getattr(delta, attr)
        if attr_val == 0:
            continue
        plur_or_sing_attr = attr if attr_val > 1 else attr[:-1]
        items.append('%d %s' % (attr_val, plur_or_sing_attr))
    # keep only 2 items max, this is enough granularity for a human.
    items = items[:2]
    return ' and '.join(items)

def main():
    if LOG_CONF.exists():
        LOG_CONF.unlink()
    config_per_site = defaultdict(lambda: {})
    for site, num_nodes in WALT_NODE_COUNTS.items():
        config_per_site[site]['num_nodes'] = num_nodes
    config_per_site[WALT_SERVER_SITE]['server'] = True
    env = { 'server': { 'site': WALT_SERVER_SITE },
            'sites': config_per_site }
    add_vlan_config(env)
    monitoring = submit(config_per_site)
    print('Waiting for grid job submission...')
    for line in monitoring:
        print(line)
        if 'batchId = ' in line:
            site = line.split(']')[1][2:]
            site_job_id = line.split()[-1]
            config_per_site[site]['job_id'] = site_job_id
        elif 'Grid reservation id' in line:
            env['grid_job_id'] = line.split()[-1]
            break
    if 'grid_job_id' not in env:
        print('Failed.')
        return
    print('Waiting for the jobs to run...')
    while True:
        max_start_time = 0
        waiting_sites = []
        for site in config_per_site.keys():
            job_stat = oarstat(env, site)
            if job_stat['state'] != 'Running':
                waiting_sites.append(site)
                max_start_time = max(max_start_time, job_stat['scheduledStart'])
        if len(waiting_sites) == 0:
            break
        print('\x1b[2K\rWaiting for sites: ' + \
              ', '.join(waiting_sites),  end='')
        now = int(time.time())
        if (max_start_time > now):
            print(' estimated wait: ' + \
                  human_readable_delay(max_start_time - now), end='')
    print('\x1b[2K\r')
    print('Running deployment tasks...')
    deploy_walt(env)
    print('Ready!')
    server_node = env['server']['host']
    server_node_nodomain = server_node.split('.')[0]
    cmd = cmd_on_site(WALT_SERVER_SITE, f"ssh root@{server_node_nodomain}", tty=True)
    env['server_access_cmd'] = cmd
    print('You can connect to walt server by using:')
    print('$ ' + cmd)
    LOG_CONF.write_text(json.dumps(env))
    print(f'For more info (selected nodes, etc.), checkout this json file: {LOG_CONF}')

def get_node_info(node_hostname):
    node_nodomain, site = node_hostname.split('.')[:2]
    node_cluster = node_nodomain.rsplit('-', maxsplit=1)[0]
    node_api = f'https://api.grid5000.fr/sid/sites/{site}/clusters/{node_cluster}/nodes/{node_nodomain}.json'
    resp = requests.get(node_api)
    return resp.json()

def send_json_conf_to_server(env):
    server_node = env['server']['host']
    server_info = get_node_info(server_node)
    server_eth1_name = server_info['network_adapters'][1]['name']
    nodes_info = []
    for site, site_info in env['sites'].items():
        for node in site_info['nodes']:
            node_info = get_node_info(node)
            node_nodomain = node.split('.')[0]
            node_mac = node_info['network_adapters'][0]['mac']
            nodes_info.append((node_nodomain, node_mac))
    json_conf = json.dumps({
        'server_eth1_name': server_eth1_name,
        'nodes': nodes_info
    })
    # write this conf to the server
    run_cmd_on_site(env, env['server']['site'],
                f'ssh root@{server_node} tee /tmp/g5k.json',
                input=json_conf)

def deploy_walt(env):
    update_vlan_id(env)
    vlan_id = env['vlan']['vlan_id']
    vlan_site = env['vlan']['site']
    update_nodes(env)
    server_site = env['server']['site']
    server_node = env['server']['host']
    server_node_eth1 = '-eth1.'.join(server_node.split('.',maxsplit=1))
    print(f'Removing default DHCP service on VLAN {vlan_id}...')
    run_cmd_on_site(env, vlan_site, f'kavlan -d -i {vlan_id}', True)
    # deploy server
    print(f'Deploying walt server on node {server_node}... (this will take ~5min)')
    run_cmd_on_site(env, server_site,
                f'kadeploy3 -m {server_node} -a {WALT_SERVER_ENV_FILE}')
    print(f'Attaching walt server secondary interface to VLAN {vlan_id} (walt-net)...')
    run_cmd_on_site(env, server_site,
                f'kavlan -s -i {vlan_id} -m {server_node_eth1}', True)
    # configure server
    print(f'Configuring walt server...')
    send_json_conf_to_server(env)
    run_cmd_on_site(env, server_site,
                f'ssh root@{server_node} /root/walt-server-setup.py /tmp/g5k.json')
    # boot walt nodes
    print(f'Attaching walt nodes to VLAN {vlan_id} (walt-net) and rebooting them...')
    for site, site_info in env['sites'].items():
        for node in site_info['nodes']:
            run_cmd_on_site(env, site, f'kavlan -s -i {vlan_id} -m {node}', True)
            run_cmd_on_site(env, site, f'kareboot3 --no-wait -l hard -m {node}')

if __name__ == "__main__":
    main()
