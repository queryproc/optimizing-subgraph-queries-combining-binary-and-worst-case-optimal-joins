#!/usr/bin/env python3
import os
import subprocess
import argparse

bin_home = os.environ['GRAPHFLOW_HOME'] + '/build/install/graphflow/bin/'

def main():
    args = parse_args()
    # set SerializeDatasetRunner.java arguments and exectue the binary.
    dataset_serializer = [
        bin_home + 'dataset-serializer',
        '-e', args.input_file_edges, '-o', args.output_graph]
    if args.input_file_vertices:
        dataset_serializer.extend(['-v', args.input_file_vertices])
    if args.edges_file_separator:
        dataset_serializer.extend(['-m', args.edges_file_separator])
    if args.vertices_file_separator:
        dataset_serializer.extend(['-n', args.vertices_file_separator])

    # SerializeDatasetRunner from
    # Graphflow-Optimizers/src/ca.waterloo.dsg.graphflow.runner.
    # dataset.DatasetSerializer:
    #     1) loads the csv file.
    #     2) gets stats about the dataset for the optimizer.
    #     3) serealizes the graph, the stats, and type store.
    popen = subprocess.Popen(
        tuple(dataset_serializer), stdout=subprocess.PIPE)
    popen.wait()
    for line in iter(popen.stdout.readline, b''):
        print(line.decode("utf-8"), end='')

def parse_args():
    parser = argparse.ArgumentParser(
        description='loads the csv files as a graph and serialize it.')
    parser.add_argument('input_file_edges',
        help='absolute path to the input edges csv file.')
    parser.add_argument('output_graph',
        help='aboluste path to the output serialized graph directory.')
    parser.add_argument('-v', '--input_file_vertices',
        help='absolute path to the input vertices csv file.')
    parser.add_argument('-e', '--edges_file_separator',
        help='csv separator in the input edges csv file.')
    parser.add_argument('-s', '--vertices_file_separator',
        help='csv separator in the input vertices csv file.')
    return parser.parse_args()

if __name__ == '__main__':
    main()

