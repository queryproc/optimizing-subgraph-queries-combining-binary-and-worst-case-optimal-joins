package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.storage.SortedAdjList;
import ca.waterloo.dsg.graphflow.util.collection.MapUtils;
import lombok.Getter;
import lombok.var;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Scans all edges in the forward adjacency list given an edge label, a source vertex type, and a
 * destination source type. Scanned edge are pushed to the next operators one at a time.
 */
public class Scan extends Operator implements Serializable {

    @Getter protected String fromQueryVertex, toQueryVertex;
    @Getter short fromType, toType, labelOrToType;

    SortedAdjList[] fwdAdjList;
    private int fromVertexStartIdx, fromVertexEndIdx;
    int[] vertexIds;
    short[] vertexTypes;

    /**
     * Constructs a {@link Scan} operator.
     *
     * @param outSubgraph is the subgraph matched by the scanned output tuples.
     */
    public Scan(QueryGraph outSubgraph) {
        super(outSubgraph, null /* no inSubgraph */);
        if (outSubgraph.getEdges().size() > 1) {
            throw new IllegalArgumentException();
        }
        var queryEdge = outSubgraph.getEdges().get(0);
        fromType = queryEdge.getFromType();
        toType = queryEdge.getToType();
        labelOrToType = queryEdge.getLabel();
        lastRepeatedVertexIdx = 0;
        fromQueryVertex = queryEdge.getFromVertex();
        toQueryVertex = queryEdge.getToVertex();
        outQVertexToIdxMap = new HashMap<>();
        outQVertexToIdxMap.put(fromQueryVertex, 0);
        outQVertexToIdxMap.put(toQueryVertex,   1);
        name = "SCAN (" + fromQueryVertex + ")->(" + toQueryVertex + ")";
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        this.probeTuple = probeTuple;
        this.vertexIds = graph.getVertexIds();
        this.vertexTypes = graph.getVertexTypes();
        if (KeyStore.ANY != fromType) {
            this.fromVertexStartIdx = graph.getVertexTypeOffsets()[fromType];
            this.fromVertexEndIdx = graph.getVertexTypeOffsets()[fromType + 1];
        } else {
            this.fromVertexStartIdx = 0;
            this.fromVertexEndIdx = graph.getHighestVertexId() + 1;
        }
        this.fwdAdjList = graph.getFwdAdjLists();
        if (graph.isAdjListSortedByType()) {
            labelOrToType = toType;
            toType = KeyStore.ANY;
        }
        for (var nextOperator : next) {
            nextOperator.init(probeTuple, graph, store);
        }
    }

    /**
     * @see Operator#execute()
     */
    @Override
    public void execute() throws LimitExceededException {
        int fromVertex, toVertexStartIdx, toVertexEndIdx;
        for (var fromIdx = fromVertexStartIdx; fromIdx < fromVertexEndIdx; fromIdx++) {
            fromVertex = vertexIds[fromIdx];
            probeTuple[0] = fromVertex;
            toVertexStartIdx = fwdAdjList[fromVertex].getLabelOrTypeOffsets()[labelOrToType];
            toVertexEndIdx = fwdAdjList[fromVertex].getLabelOrTypeOffsets()[labelOrToType + 1];
            for (var toIdx = toVertexStartIdx; toIdx < toVertexEndIdx; toIdx++) {
                probeTuple[1] = fwdAdjList[fromVertex].getNeighbourId(toIdx);
                if (toType == KeyStore.ANY || vertexTypes[probeTuple[1]] == toType) {
                    numOutTuples++;
                    next[0].processNewTuple();
                }
            }
        }
    }

    /**
     * @see Operator#updateOperatorName(Map)
     */
    @Override
    public void updateOperatorName(Map<String, Integer> queryVertexToIndexMap) {
        queryVertexToIndexMap = new HashMap<>();
        queryVertexToIndexMap.put(fromQueryVertex, 0);
        queryVertexToIndexMap.put(toQueryVertex, 1);
        if (null != next) {
            for (var nextOperator : next) {
                nextOperator.updateOperatorName(MapUtils.copy(queryVertexToIndexMap));
            }
        }
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() {
        throw new UnsupportedOperationException(
            this.getClass().getSimpleName() + " does not support execute().");
    }

    /**
     * @see Operator#copy(boolean)
     */
    @Override
    public Scan copy(boolean isThreadSafe) {
        if (isThreadSafe) {
            return new ScanBlocking(outSubgraph);
        }
        return new Scan(outSubgraph);
    }

    /**
     * @see Operator#isSameAs(Operator)
     */
    public boolean isSameAs(Operator operator) {
        return operator instanceof Scan &&
            fromType == ((Scan) operator).fromType &&
            toType == ((Scan) operator).toType &&
            labelOrToType == ((Scan) operator).labelOrToType;
    }
}
