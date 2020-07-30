package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.planner.catalog.Catalog;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.storage.SortedAdjList;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Given a set of input tuples from the prev {@link Operator}, E/I extends the tuples by one query
 * vertex.
 */
public class Extend extends EI implements Serializable {

    private int vertexIndex;
    private short labelOrToType;
    private Direction dir;
    private SortedAdjList[] adjList;

    /**
     * @see EI#make(String, short, List, QueryGraph, QueryGraph, Map)
     */
    Extend(String toQVertex, short toType, List<AdjListDescriptor> ALDs,
        QueryGraph outSubgraph, QueryGraph inSubgraph, Map<String, Integer> outQVertexToIdxMap) {
        super(toQVertex, toType, ALDs, outSubgraph, inSubgraph);
        var ALD = ALDs.get(0);
        this.vertexIndex = ALD.getVertexIdx();
        this.dir = ALD.getDirection();
        this.labelOrToType = ALD.getLabel();
        this.lastRepeatedVertexIdx = outTupleLen - 2;
        this.outQVertexToIdxMap = outQVertexToIdxMap;
        this.outIdx = outQVertexToIdxMap.get(toQVertex);
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        logger.info("init 1 type: " + toType);
        this.outNeighbours = new Neighbours();
        this.probeTuple = probeTuple;
        this.vertexTypes = graph.getVertexTypes();
        this.adjList = dir == Direction.Fwd ? graph.getFwdAdjLists() : graph.getBwdAdjLists();
        if (graph.isAdjListSortedByType()) {
            labelOrToType = toType;
            toType = KeyStore.ANY;
        }
        for (var nextOperator : next) {
            nextOperator.init(probeTuple, graph, store);
        }
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    @SuppressWarnings("fallthrough")
    public void processNewTuple() throws LimitExceededException {
        adjList[probeTuple[vertexIndex]].setNeighbourIds(labelOrToType, outNeighbours);
        icost += outNeighbours.endIdx - outNeighbours.startIdx;
        for (var idx = outNeighbours.startIdx; idx < outNeighbours.endIdx; idx++) {
            if (toType == KeyStore.ANY || toType == vertexTypes[outNeighbours.Ids[idx]]) {
                numOutTuples++;
                probeTuple[outIdx] = outNeighbours.Ids[idx];
                next[0].processNewTuple();
            }
        }
    }

    /**
     * @see Operator#isSameAs(Operator)
     */
    @Override
    public boolean isSameAs(Operator operator) {
        if (!(operator instanceof Extend)) {
            return false;
        }
        var extend = (Extend) operator;
        return
            this == extend || (
                (!DIFFERENTIATE_FWD_BWD_SINGLE_ALD || dir == extend.dir) &&
                labelOrToType == extend.labelOrToType &&
                toType == extend.toType &&
                inSubgraph.isIsomorphicTo(operator.getInSubgraph()) &&
                outSubgraph.isIsomorphicTo(operator.getOutSubgraph()) &&
                prev.isSameAs(operator.getPrev())
        );
    }

    /**
     * @see Operator#copy(boolean)
     */
    @Override
    public Extend copy(boolean isThreadSafe) {
        logger.info("copy Extend - toType: " + toType);
        var extend = new Extend(toQueryVertex, toType, ALDs, outSubgraph, inSubgraph,
            outQVertexToIdxMap);
        extend.prev = prev.copy(isThreadSafe);
        extend.prev.setNext(extend);
        extend.initCaching(extend.prev.getLastRepeatedVertexIdx());
        return extend;
    }
}
