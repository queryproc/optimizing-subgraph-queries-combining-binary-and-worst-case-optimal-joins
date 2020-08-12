#!/usr/bin/env python3
"""
Given an absolute path to a directory containing '.txt' files from The Stanford
Large Network Dataset Collection, the script outputs for each file in the
directory an edges '.csv' files. The CSV is later loaded to graphflow and saved
to a directory in binary format.
"""
import sys
import argparse
import random
from os import listdir
from os.path import isfile, join

def main():
    args = parse_args()
    highestVertexId = produce_edges_file(args.input_file,
        args.output_edges_file, args.separator, args.label)
    if args.output_vertices_file:
	    pass

def parse_args():
    parser = argparse.ArgumentParser(description='reads ')
    parser.add_argument('input_file',
        help='the raw input file using absolute path')
    parser.add_argument('output_edges_file',
        help='the csv edges output file using absolute path.')
    parser.add_argument('-o', '--output_vertices_file',
	help='the csv vertices output file using absolute path.')
    parser.add_argument('-s', '--separator',
        help='separator between vertices in each line.', default='\t')
    parser.add_argument('-t', '--type',
        help='number of vertex types.', type=int, default=1)
    parser.add_argument('-l', '--label',
        help='number of edge labels.', type=int, default=1)
    return parser.parse_args()

def produce_edges_file(input_file, output_file, separator, num_of_labels):
    edges_file = open(output_file, 'w+')
    highestVertexId = -1
    # format file written as: FROM,TO,LABEL.
    random.seed(0) # use '0' to always get the same sequence of labels  
    with open(input_file) as f:
        for line in f:
            if line[0] == '#': # read comment and remove, process the rest.
                continue
            try:
                edge = line.split(separator)
                if len(edge) == 1:
                    edge = line.split(' ') # edge=['<from>','<to>\n']
                fromVertex = edge[0]
                toVertex = edge[1]
                toVertex = toVertex[:len(toVertex)-1] # removes '\n'
                if int(fromVertex) > highestVertexId:
                    highestVertexId = int(fromVertex)
                if int(toVertex) > highestVertexId:
                    highestVertexId = int(toVertex)
            except Exception: # does not follow the usual csv pattern
                continue
            if fromVertex == toVertex: # remove self-loops
                continue
            edge_label = random.randint(0, num_of_labels - 1)
            edges_file.write(fromVertex + ',' + toVertex + ',' + \
                             str(edge_label) + '\n')
    edges_file.close()

def produce_vertices_file(input_file, output_file, separator, num_of_types,
    highestVertexId):
    vertices_file = open(output_file, 'w+')
    # format file written as: VERTEX_ID,TYPE.
    random.seed(0) # use '0' to always get the same sequence of types
    for vertexId in range(0, highestVertexId + 1):
        vertex_type = random.randint(0, num_of_types - 1)
        vertices_file.write(str(vertexId) + ',' + str(vertex_type) + '\n')
    vertices_file.close()

if __name__ == '__main__':
    main()
