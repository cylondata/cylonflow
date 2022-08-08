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


def stop_ray(host_names, args):
    head = host_names[0]

    import ray
    ray.shutdown()

    print("stopping workers", flush=True)
    for ip in host_names[1:]:
        subprocess.run(f"ssh {ip} {ray_exec} stop -f", stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, shell=True)

    time.sleep(3)

    print("stopping head", flush=True)
    q = f"{ray_exec} stop -f"
    if args['host_file']:
        q = _with_ssh(q, head)
    subprocess.run(q, stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT, shell=True)

    time.sleep(3)


def status_ray(host_names, args):
    head = host_names[0]
    query = f"{ray_exec} status --redis_password={args['redis_pw']}"

    if args['host_file']:
        query = _with_ssh(query, head)
    p = subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True,
                       text=True)
    print(p.stdout)


def run_main(args):
    host_names = []
    if args['host_file']:
        with open(args['host_file'], 'r') as fp:
            for line in fp.readlines():
                host_names.append(line.split(' ')[0])
    else:
        host_names = ['localhost']

    total_nodes = len(host_names)

    if args['op'] == 'start':
        w = args['procs']
        procs_per_node = max(int(math.ceil(w / total_nodes)), 1)
        start_ray(host_names, procs_per_node, min(w, total_nodes), args)
    elif args['op'] == 'stop':
        stop_ray(host_names, args)
    elif args['op'] == 'status':
        status_ray(host_names, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='manage ray')
    parser.add_argument('op', choices=['start', 'stop', 'status'])

    parser.add_argument('-f', dest='host_file', type=int, help='nodes file path', required=False,
                        default=None)
    parser.add_argument('-n', dest='procs', type=int, help='total processes', required=False,
                        default=1)
    parser.add_argument('--redis-pw', dest='redis_pw', type=str, help='engine', required=False,
                        default='1234')

    args_ = vars(parser.parse_args())

    run_main(args_)
