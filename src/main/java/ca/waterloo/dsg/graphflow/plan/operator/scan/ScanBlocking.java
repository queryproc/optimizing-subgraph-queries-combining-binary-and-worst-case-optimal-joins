package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A multi-threaded implementation of the {@link Scan} operator .
 */
public class ScanBlocking extends Scan {

    protected static final Logger logger = LogManager.getLogger(ScanBlocking.class);

    public static int PARTITION_SIZE = 100;

    private int currFromIdx, currToIdx;
    private int fromIdxLimit, toIdxLimit;
    private int highestFromIdx, highestToIdx;

    @Setter private VertexIdxLimits globalVerticesIdxLimits;

    public static class VertexIdxLimits {
        int fromVariableIndexLimit;
        int toVariableIndexLimit;
        ReentrantLock lock = new ReentrantLock();
    }

    /**
     * Constructs a {@link Scan} operator.
     *
     * @param outputSubgraph The subgraph, with one query relation, matched by the output tuples.
     */
    ScanBlocking(QueryGraph outputSubgraph) {
        super(outputSubgraph);
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        this.probeTuple = probeTuple;
        this.vertexIds = graph.getVertexIds();
        this.vertexTypes = graph.getVertexTypes();
        this.fwdAdjList = graph.getFwdAdjLists();
        if (graph.isAdjListSortedByType()) {
            labelOrToType = toType;
            toType = KeyStore.ANY;
        }
        if (KeyStore.ANY != fromType) {
            currFromIdx = graph.getVertexTypeOffsets()[fromType];
            highestFromIdx = graph.getVertexTypeOffsets()[fromType + 1];
        } else {
            currFromIdx = 0;
            highestFromIdx = graph.getHighestVertexId() + 1;
        }
        currToIdx = fwdAdjList[vertexIds[currFromIdx]].getLabelOrTypeOffsets()[labelOrToType];
        highestToIdx = fwdAdjList[vertexIds[highestFromIdx - 1]].getLabelOrTypeOffsets()[
            labelOrToType + 1];
        globalVerticesIdxLimits.fromVariableIndexLimit = currFromIdx;
        globalVerticesIdxLimits.toVariableIndexLimit = currToIdx;
        for (var nextOperator : next) {
            nextOperator.init(probeTuple, graph, store);
        }
    }

    /**
     * @see Operator#execute()
     */
    @Override
    public void execute() throws LimitExceededException {
        updateIndicesLimits();
        while (currFromIdx < highestFromIdx - 1 ||
            (currFromIdx == highestFromIdx - 1 && currToIdx < highestToIdx)) {
            if (currFromIdx == fromIdxLimit) {
                produceNewEdges(currFromIdx, currToIdx, toIdxLimit);
            } else if (currFromIdx < fromIdxLimit) {
                produceNewEdges(currFromIdx, currToIdx, fwdAdjList[vertexIds[currFromIdx]].
                    getLabelOrTypeOffsets()[labelOrToType + 1]);
                for (var fromIdx = currFromIdx + 1; fromIdx < fromIdxLimit; fromIdx++) {
                    var adjList = fwdAdjList[vertexIds[fromIdx]];
                    produceNewEdges(fromIdx, adjList.getLabelOrTypeOffsets()[labelOrToType],
                        adjList.getLabelOrTypeOffsets()[labelOrToType + 1]);
                }
                produceNewEdges(fromIdxLimit, fwdAdjList[vertexIds[fromIdxLimit]].
                    getLabelOrTypeOffsets()[labelOrToType], toIdxLimit);
            }
            updateIndicesLimits();
        }
    }

    private void produceNewEdges(int fromIdx, int startToIdx, int endToIdx)
        throws LimitExceededException {
        probeTuple[0] = vertexIds[fromIdx];
        for (var toIdx = startToIdx; toIdx < endToIdx; toIdx++) {
            probeTuple[1] = fwdAdjList[probeTuple[0]].getNeighbourId(toIdx);
            if (toType == KeyStore.ANY || vertexTypes[probeTuple[1]] == toType) {
                numOutTuples++;
                next[0].processNewTuple();
            }
        }
    }

    private void updateIndicesLimits() {
        globalVerticesIdxLimits.lock.lock();
        try {
            fromIdxLimit = currFromIdx = globalVerticesIdxLimits.fromVariableIndexLimit;
            toIdxLimit   = currToIdx   = globalVerticesIdxLimits.toVariableIndexLimit;
            var numEdgesLeft = PARTITION_SIZE;
            while (numEdgesLeft > 0 && (fromIdxLimit < highestFromIdx ||
                (fromIdxLimit == highestFromIdx - 1 && toIdxLimit < highestToIdx))) {
                var toLimit = fwdAdjList[vertexIds[fromIdxLimit]].getLabelOrTypeOffsets()[
                    labelOrToType + 1];
                if (toIdxLimit + numEdgesLeft < toLimit) {
                    toIdxLimit += numEdgesLeft;
                    numEdgesLeft = 0;
                } else { // toIdxLimit + numEdgesLeft >= toLimit
                    numEdgesLeft -= (toLimit - toIdxLimit + 1);
                    toIdxLimit = toLimit;
                    if (fromIdxLimit == highestFromIdx - 1) {
                        break;
                    }
                    fromIdxLimit += 1;
                    toIdxLimit = fwdAdjList[vertexIds[fromIdxLimit]].getLabelOrTypeOffsets()[
                        labelOrToType];
                }
            }
            globalVerticesIdxLimits.fromVariableIndexLimit = fromIdxLimit;
            globalVerticesIdxLimits.toVariableIndexLimit = toIdxLimit;
        } finally {
            globalVerticesIdxLimits.lock.unlock();
        }
    }
}
