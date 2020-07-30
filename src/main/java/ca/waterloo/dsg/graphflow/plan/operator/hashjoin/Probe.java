package ca.waterloo.dsg.graphflow.plan.operator.hashjoin;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.HashTable.BlockInfo;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.Getter;
import lombok.Setter;
import lombok.var;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An operator matching incoming tuples by probing a hash table on multiple attributes.
 */
public class Probe extends Operator implements Serializable {

    @Getter @Setter HashTable[] hashTables;

    @Getter @Setter int ID;

    List<String> joinQVertices;
    int probeHashIdx;
    int hashedTupleLen;
    int probeTupleLen;

    transient BlockInfo blockInfo;

    /**
     * Constructs a {@link Probe} operator.
     *
     * @param outSubgraph is the subgraph matched by the output tuples.
     * @param inSubgraph is the subgraph matched by the input tuples.
     * @param joinQVertices is the list of query vertices to probe the hash table tuples on.
     * @param probeHashIdx is the index to probe the hash table on.
     * @param hashedTupleLen is the length of the hashed tuple to copy.
     * @param probeTupleLen is the length of the previous operator output.
     * @param outQVertexToIdxMap The output query vertex to tuple index map.
     */
    Probe(QueryGraph outSubgraph, QueryGraph inSubgraph, List<String> joinQVertices,
        int probeHashIdx, int hashedTupleLen, int probeTupleLen,
        Map<String, Integer> outQVertexToIdxMap) {
        this.outSubgraph = outSubgraph;
        this.inSubgraph = inSubgraph;
        this.joinQVertices = joinQVertices;
        this.probeHashIdx = probeHashIdx;
        this.hashedTupleLen = hashedTupleLen;
        this.probeTupleLen = probeTupleLen;
        this.outQVertexToIdxMap = outQVertexToIdxMap;
        this.outTupleLen = outQVertexToIdxMap.size();
        name = "PROBE ON (" + joinQVertices.get(0) + ")";
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        if (null == this.probeTuple) {
            this.probeTuple = probeTuple;
            this.blockInfo = new BlockInfo();
            for (var nextOperator : next) {
                nextOperator.init(probeTuple, graph, store);
            }
        }
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() throws LimitExceededException {
        var hashVertex = probeTuple[probeHashIdx];
        for (var hashTable : hashTables) {
            var lastChunkIdx = hashTable.numChunks[hashVertex];
            var prevFirstItem = -1;
            for (var chunkIdx = 0; chunkIdx < lastChunkIdx; chunkIdx++) {
                hashTable.getBlockAndOffsets(hashVertex, chunkIdx, blockInfo);
                for (var offset = blockInfo.startOffset; offset < blockInfo.endOffset;) {
                    numOutTuples++;
                    if (hashedTupleLen == 2) {
                        var firstItem = blockInfo.block[offset++];
                        if (prevFirstItem != firstItem) {
                            probeTuple[probeTupleLen] = firstItem;
                            prevFirstItem = firstItem;
                        }
                        probeTuple[probeTupleLen + 1] = blockInfo.block[offset++];
                    } else {
                        for (var k = 0; k < hashedTupleLen; k++) {
                            probeTuple[probeTupleLen + k] = blockInfo.block[offset++];
                        }
                    }
                    next[0].processNewTuple();
                }
            }
        }
    }

    /**
     * @see Operator#isSameAs(Operator)
     */
    public boolean isSameAs(Operator operator) {
        return this == operator || (operator instanceof Probe &&
            inSubgraph.isIsomorphicTo(operator.getInSubgraph()) &&
            outSubgraph.isIsomorphicTo(operator.getOutSubgraph()) &&
            prev.isSameAs(operator.getPrev())
        );
    }

    /**
     * @see Operator#copy(boolean)
     */
    public Probe copy(boolean isThreadSafe) {
        var probe = new Probe(outSubgraph, inSubgraph, joinQVertices, probeHashIdx, hashedTupleLen,
            probeTupleLen, outQVertexToIdxMap);
        probe.prev = prev.copy(isThreadSafe);
        probe.prev.setNext(probe);
        probe.setID(ID);
        return probe;
    }
}
