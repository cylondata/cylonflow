#!/usr/bin/env python

import argparse
import math
import os
import subprocess
import sys
import tempfile
import time

python_exec = sys.executable
prefix = sys.prefix
home_dir = os.path.expanduser("~")
temp_dir = tempfile.gettempdir()
ray_exec = f'{prefix}/bin/ray'

# TOTAL_NODES = 14
# MAX_PROCS = 40
# TOTAL_MEM = 240
# RAY_PW = '1234'
# # RAY_EXEC = "/N/u2/d/dnperera/victor/MODIN/bin/ray"
# RAY_EXEC = "/N/u2/d/dnperera/victor/modin_env/bin/ray"
# HEAD_IP = "v-001"
#
# DASK_SCHED = "/N/u2/d/dnperera/victor/modin_env/bin/dask-scheduler"
# SCHED_FILE = "/N/u2/d/dnperera/dask-sched.json"
# DASK_WORKER = "/N/u2/d/dnperera/victor/modin_env/bin/dask-worker"
# SCHED_IP = "v-001"
#
# nodes_file = "nodes.txt"
# ips = []
#
#
#
# assert len(ips) == TOTAL_NODES

def _with_ssh(q, ip):
    return f'ssh {ip} {q}'


def start_ray(host_names, procs_per_node, nodes, args):
    redis_pw = args['redis_pw']

    head = host_names[0]

    print("starting head", flush=True)
    query = f"{ray_exec} start --head --port=6379 --node-ip-address={head} " \
            f"--redis-password={redis_pw} --num-cpus={max(2, procs_per_node)}"
    if args['host_file']:
        query = _with_ssh(query, head)
    print(f"running: {query}", flush=True)
    subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, check=True)

    time.sleep(3)

    print(f"starting workers", flush=True)
    for ip in host_names[1:nodes]:
        query = f"ssh {ip} {ray_exec} start --address=\'{head}:6379\' --node-ip-address={ip} " \
                f"--redis-password={redis_pw} --num-cpus={procs_per_node}"
        print(f"running: {query}", flush=True)
        subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True,
                       check=True)

    time.sleep(3)


def stop_ray(head, prefix):
    ray_exec = f'{prefix}/bin/ray' if prefix else RAY_EXEC
    if not head:
        head = HEAD_IP

    import ray
    ray.shutdown()

    print("stopping workers", flush=True)
    for ip in host_names:
        subprocess.run(f"ssh {ip} {ray_exec} stop -f", stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, shell=True)

    time.sleep(3)

    print("stopping head", flush=True)
    subprocess.run(f"ssh {head} {ray_exec} stop -f", stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT, shell=True)

    time.sleep(3)


def run_main(args):
    host_names = []
    if args['host_file']:
        with open(args['host_file'], 'r') as fp:
            for line in fp.readlines():
                host_names.append(line.split(' ')[0])
    else:
        host_names = ['localhost']

    head = host_names[0]
    total_nodes = len(host_names)

    if args['op'] == 'start':
        w = args['procs']
        procs_per_node = int(math.ceil(w / total_nodes))
        start_ray(host_names, procs_per_node, min(w, total_nodes), args)
    elif args['op'] == 'stop':
        stop_cluster(args['engine'], head, prefix)
    elif args['op'] == 'status':
        cluster_status(args['engine'], head, prefix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='manage ray')
    parser.add_argument('op', choices=['start', 'stop', 'status'])

    parser.add_argument('-f', dest='host_file', type=int, help='nodes file path', required=False,
                        default=None)
    parser.add_argument('-n', dest='procs', type=int, help='total processes', required=False,
                        default=0)
    # parser.add_argument('-e', dest='engine', type=str, help='engine', required=False, default='ray')
    #
    # dask_args = parser.add_argument_group("Dask")
    # parser.add_argument('-m', dest='mem', type=int, help='memory per node (GB)', required=False,
    #                     default=4)
    # dask_args.add_argument('--sched-file', dest='sched-file', type=str, help='scheduler file',
    #                        required=False, default=f"{home_dir}/dask-sched.json")
    # dask_args.add_argument('--local-dir', dest='local-dir', type=str, help='local dir',
    #                        required=False, default=f"{temp_dir}/dask")
    # dask_args.add_argument('-i', dest='interface', type=str, help='interface', required=False,
    #                        default=None)

    # ray_args = parser.add_argument_group("Ray")
    parser.add_argument('--redis-pw', dest='redis_pw', type=str, help='engine', required=False,
                        default='1234')

    args_ = vars(parser.parse_args())

    run_main(args_)
