#!/usr/bin/env python3
import os
import glob
import subprocess
import argparse
import shutil
import random

bin_home = os.environ['GRAPHFLOW_HOME'] + '/build/install/graphflow/bin/'

def main():
    args = parse_args()
    print(args.query)
    # set OptimizerRunner.java arguments and exectue the binary.
    binary_and_args = [
        bin_home + 'optimizer-executor',
        '-q', args.query, '-i', args.input_graph, '-t', str(args.threads), '-e']
    # OptimizerExecutor from
    # Graphflow-Optimizers/src/ca.waterloo.dsg.graphflow.runner.plan:
    #     1) gets a query plan using QueryPlanner.
    #     2) Output is logged to STDOUT.
    popen = subprocess.Popen(tuple(binary_and_args), stdout=subprocess.PIPE)
    popen.wait()
    for line in iter(popen.stdout.readline, b''):
        print(line.decode("utf-8"), end='')

def parse_args():
    parser = argparse.ArgumentParser(
        description='runs the optimizer to evaluate a query.')
    parser.add_argument('query', help='query graph to evaluate.')
    parser.add_argument('input_graph',
        help='aboluste path to the serialized input graph directory.')
    parser.add_argument('-t', '--threads',
        help='number of execution threads.', type=int, default=1)
    parser.add_argument('-e', '--execute',
        help='execute the plan.', action="store_true")
    return parser.parse_args()

if __name__ == '__main__':
    main()

