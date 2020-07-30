#!/usr/bin/env python3
import os
import subprocess
import argparse

bin_home = os.environ['GRAPHFLOW_HOME'] + '/build/install/graphflow/bin/'

def main():
    args = parse_args()
    # set SerializeCatalogRunner.java arguments and exectue the binary.
    serialize_catalog_runner = [
        bin_home + 'catalog-serializer', '-i', args.input_graph]
    if args.num_edges:
        serialize_catalog_runner.append('-n')
        serialize_catalog_runner.append(str(args.num_edges))
    if args.vertices:
        serialize_catalog_runner.append('-v')
        serialize_catalog_runner.append(str(args.vertices))
    if args.threads:
        serialize_catalog_runner.append('-t')
        serialize_catalog_runner.append(str(args.threads))

    # SerializeCatalogRunner from
    # Graphflow-Optimizers/src/ca.waterloo.dsg.graphflow.runner.plan:
    #     1) loads the serialized input graph.
    #     2) gets stats about the dataset for the optimizer.
    #     3) serealizes the stats data.
    popen = subprocess.Popen(
        tuple(serialize_catalog_runner), stdout=subprocess.PIPE)
    popen.wait()
    for line in iter(popen.stdout.readline, b''):
        print(line.decode("utf-8"), end='')

def parse_args():
    parser = argparse.ArgumentParser(
        description='loads the serialized input graph.')
    parser.add_argument('input_graph',
        help='aboluste path to serialized input graph directory.')
    parser.add_argument('-n', '--num_edges',
        help='number of edges to sample when scanning for the catalog stats.',
            type=int)
    parser.add_argument('-v', '--vertices',
        help='max number of vertices input subgraphs when collecting stats.',
            type=int)
    parser.add_argument('-t', '--threads',
        help='number of threads to use when parallelizing stats collection.',
            type=int)
    return parser.parse_args()

if __name__ == '__main__':
    main()

