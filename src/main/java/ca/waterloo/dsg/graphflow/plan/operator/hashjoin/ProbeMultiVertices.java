package ca.waterloo.dsg.graphflow.plan.operator.hashjoin;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import lombok.var;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An operator matching incoming tuples by probing a hash table on multiple attributes.
 */
public class ProbeMultiVertices extends Probe implements Serializable {

    int[] probeIndices;
    int[] buildIndices;

    /**
     * Constructs a {@link ProbeMultiVertices} operator.
     *
     * @param outSubgraph is the subgraph matched by the output tuples.
     * @param inSubgraph is the subgraph matched by the input tuples.
     * @param joinQVertices is the list of variables to probe the hash table on.
     * @param probeHashIdx is the index to probe the hash table on.
     * @param probeIndices is the setAdjListSortOrder of extra indices to check for equality on.
     * @param buildIndices is the setAdjListSortOrder of indices to match on with the hashTable.
     * @param hashedTupleLen is the length of the hashed tuple to copy.
     * @param probeTupleLen is the length of the previous operator output.
     * @param outQVertexToIdxMap The output query vertex to tuple index map.
     */
    ProbeMultiVertices(QueryGraph outSubgraph, QueryGraph inSubgraph, List<String> joinQVertices,
        int probeHashIdx, int[] probeIndices, int[] buildIndices, int hashedTupleLen,
        int probeTupleLen, Map<String, Integer> outQVertexToIdxMap) {
        super(outSubgraph, inSubgraph, joinQVertices, probeHashIdx, hashedTupleLen, probeTupleLen,
            outQVertexToIdxMap);
        this.probeIndices = probeIndices;
        this.buildIndices = buildIndices;
        var strBuilder = new StringBuilder();
        strBuilder.append("PROBE ON ");
        if (1 == joinQVertices.size()) {
            strBuilder.append("(").append(joinQVertices.get(0)).append(")");
        } else {
            for (var i = 0; i < joinQVertices.size(); i++) {
                strBuilder
                    .append(i > 0 && i < joinQVertices.size() - 1 ? ", " : "")
                    .append(i == joinQVertices.size() - 1 ? " & " : "")
                    .append("(")
                    .append(joinQVertices.get(i))
                    .append(")");
            }
        }
        name = strBuilder.toString();
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() throws LimitExceededException {
        var hashVertex = probeTuple[probeHashIdx];
        for (var hashTable : hashTables) {
            var lastChunkIdx = hashTable.numChunks[hashVertex];
            for (var chunkIdx = 0; chunkIdx < lastChunkIdx; chunkIdx++) {
                hashTable.getBlockAndOffsets(hashVertex, chunkIdx, blockInfo);
                offsetLoop: for (var offset = blockInfo.startOffset;
                                     offset < blockInfo.endOffset  ; offset += hashedTupleLen) {
                    for (var i = 0; i < probeIndices.length; i++) {
                        if (probeTuple[probeIndices[i]] !=
                                blockInfo.block[offset + buildIndices[i]]) {
                            continue offsetLoop;
                        }
                    }
                    numOutTuples++;
                    var out = 0;
                    for (var k = 0; k < hashedTupleLen; k++) {
                        var copy = true;
                        for (var buildIdx : buildIndices) {
                            if (k == buildIdx) {
                                copy = false;
                                break;
                            }
                        }
                        if (copy) {
                            probeTuple[probeTupleLen + out++] = blockInfo.block[offset + k];
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
        return this == operator || (operator instanceof ProbeMultiVertices &&
            inSubgraph.isIsomorphicTo(operator.getInSubgraph()) &&
            outSubgraph.isIsomorphicTo(operator.getOutSubgraph()) &&
            prev.isSameAs(operator.getPrev())
        );
    }

    /**
     * @see Operator#copy(boolean)
     */
    public ProbeMultiVertices copy(boolean isThreadSafe) {
        var probe = new ProbeMultiVertices(outSubgraph, inSubgraph, joinQVertices, probeHashIdx,
            probeIndices, buildIndices, hashedTupleLen, probeTupleLen, outQVertexToIdxMap);
        probe.prev = prev.copy(isThreadSafe);
        probe.prev.setNext(probe);
        probe.setID(ID);
        return probe;
    }
}
