package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.storage.SortedAdjList;
import ca.waterloo.dsg.graphflow.util.collection.MapUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * A base operator for {@link Extend} and {@link Intersect} operators.
 */
public abstract class EI extends Operator {

    protected static final Logger logger = LogManager.getLogger(Extend.class);

    /**
     * Enum type of the caching used.
     */
    public enum CachingType {
        NONE,
        FULL_CACHING,
        PARTIAL_CACHING
    }

    public static boolean DIFFERENTIATE_FWD_BWD_SINGLE_ALD = false;

    protected short[] vertexTypes;

    @Getter protected short toType;
    @Getter protected String toQueryVertex;
    @Getter protected List<AdjListDescriptor> ALDs;

    @Setter protected int outIdx;

    protected int[] vertexIdx;
    protected int[] vertexIdxToCache;
    protected short[] labelsOrToTypes;
    protected short[] labelsOrToTypesToCache;
    protected transient SortedAdjList[][] adjLists;
    transient protected SortedAdjList[][] adjListsToCache;

    @Getter protected CachingType cachingType = CachingType.NONE;
    private boolean isIntersectionCached = false;
    private int[] lastVertexIdsIntersected;

    protected Neighbours outNeighbours; /* used to return output values for vertex */
    protected Neighbours initNeighbours;   /* used to set initial possible values     */
    protected Neighbours tempNeighbours;   /* used when intersecting (temporary)      */
    protected Neighbours cachedNeighbours; /* used to cache intersections             */

    public static class Neighbours {

        public int[] Ids;
        public int startIdx, endIdx;

        Neighbours() {}

        Neighbours(int capacity) {
            Ids = new int[capacity];
        }

        public void reset() {
            startIdx = 0;
            endIdx = 0;
        }
    }

    /**
     * Constructs a {@link EI} object.
     *
     * @param toQVertex is the query vertex the operator extends to.
     * @param toType is the type of the variable the operator extends to.
     * @param ALDs are the adjacency list descriptors representing the adj. lists from the
     * variables in the matched subgraph to the toQueryVertex.
     * @param outSubgraph is the subgraph matched by the output tuples.
     * @param inSubgraph is the subgraph matched by the input tuples.
     */
    EI(String toQVertex, short toType, List<AdjListDescriptor> ALDs, QueryGraph outSubgraph,
        QueryGraph inSubgraph) {
        super(outSubgraph, inSubgraph);
        this.toQueryVertex = toQVertex;
        this.toType = toType;
        this.ALDs = ALDs;
        setOperatorName();
    }

    /**
     * Constructs an {@link EI} operator.
     *
     * @param toVertex is the query vertex the operator extends to.
     * @param toType is the type of the variable the operator extends to.
     * @param ALDs are the {@link AdjListDescriptor}s the prefixes extended to need to follow.
     * @param outSubgraph is the subgraph matched by the output tuples.
     * @param inSubgraph is the subgraph matched by the input tuples.
     * @param outVertexToIdxMap The output variable to index in the prefixes map.
     */
    public static EI make(String toVertex, short toType, List<AdjListDescriptor> ALDs,
        QueryGraph outSubgraph, QueryGraph inSubgraph, Map<String, Integer> outVertexToIdxMap) {
        if (1 == ALDs.size()) {
            return new Extend(toVertex, toType, ALDs, outSubgraph, inSubgraph, outVertexToIdxMap);
        } else {
            return new Intersect(toVertex, toType, ALDs, outSubgraph, inSubgraph, outVertexToIdxMap);
        }
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        this.probeTuple = probeTuple;
        this.cachingType = CachingType.NONE;
        this.vertexTypes = graph.getVertexTypes();
        initCaching(this.prev.getLastRepeatedVertexIdx());
        initExtensions(graph);
        setALDsAndAdjLists(graph, this.prev.getLastRepeatedVertexIdx());
        for (var nextOperator : next) {
            nextOperator.init(probeTuple, graph, store);
        }
        if (this instanceof Intersect && graph.isAdjListSortedByType()) {
            toType = KeyStore.ANY;
        }
    }

    /**
     * Initializes the cache related fields to be used by the operator i.e. key and ALD indices.
     */
    public void initCaching(int lastRepeatedVertexIdx) {
        if (!CACHING_ENABLED || ALDs.size() == 1) {
            return;
        }
        var numCachedALDs = 0;
        for (var ALD : ALDs) {
            if (ALD.getVertexIdx() <= lastRepeatedVertexIdx) {
                numCachedALDs++;
            }
        }
        if (numCachedALDs <= 1) {
            return;
        }
        if (numCachedALDs == ALDs.size()) {
            cachingType = CachingType.FULL_CACHING;
        } else { // numCachedALDs in [2, ALDs.size()[
            cachingType = CachingType.PARTIAL_CACHING;
        }
        lastVertexIdsIntersected = new int[numCachedALDs];
        for (int i = 0; i < numCachedALDs; i++) {
            lastVertexIdsIntersected[i] = -1;
        }
    }

    /**
     * Sets the sorted adjacency lists to intersect for faster lookups.
     */
    protected void setALDsAndAdjLists(Graph graph, int lastRepeatedVertexIdx) {
        var numCachedALDs = lastVertexIdsIntersected != null ? lastVertexIdsIntersected.length :
            ALDs.size();
        vertexIdxToCache = new int[numCachedALDs];
        labelsOrToTypesToCache = new short[numCachedALDs];
        adjListsToCache = new SortedAdjList[numCachedALDs][];
        if (cachingType == CachingType.PARTIAL_CACHING) {
            vertexIdx = new int[ALDs.size() - numCachedALDs];
            labelsOrToTypes = new short[ALDs.size() - numCachedALDs];
            adjLists = new SortedAdjList[ALDs.size() - numCachedALDs][];
        }
        var idx = 0;
        var idxToCache = 0;
        for (var ALD : ALDs) {
            if (cachingType != CachingType.PARTIAL_CACHING ||
                    ALD.getVertexIdx() <= lastRepeatedVertexIdx) {
                vertexIdxToCache[idxToCache] = ALD.getVertexIdx();
                labelsOrToTypesToCache[idxToCache] = graph.isAdjListSortedByType() ?
                    toType : ALD.getLabel();
                adjListsToCache[idxToCache++] = ALD.getDirection() == Direction.Fwd ?
                    graph.getFwdAdjLists() : graph.getBwdAdjLists();
            } else if (cachingType == CachingType.PARTIAL_CACHING &&
                ALD.getVertexIdx() > lastRepeatedVertexIdx) {
                vertexIdx[idx] = ALD.getVertexIdx();
                labelsOrToTypes[idx] = graph.isAdjListSortedByType() ? toType : ALD.getLabel();
                adjLists[idx++] = ALD.getDirection() == Direction.Fwd ? graph.getFwdAdjLists() :
                    graph.getBwdAdjLists();
            }
        }
    }

    /**
     * Initializes the extension data structured used when intersecting.
     */
    protected void initExtensions(Graph graph) {
        if (cachingType == CachingType.NONE || cachingType == CachingType.FULL_CACHING) {
            outNeighbours = new Neighbours();
            if (1 == ALDs.size()) {
                return;
            }
        }
        var largestAdjListSize = Integer.MIN_VALUE;
        for (var ALD : ALDs) {
            var adjListSize = graph.getLargestAdjListSize(
                graph.isAdjListSortedByType() ? toType : ALD.getLabel(), ALD.getDirection());
            if (adjListSize > largestAdjListSize) {
                largestAdjListSize = adjListSize;
            }
        }
        if (cachingType == CachingType.PARTIAL_CACHING) {
            outNeighbours = new Neighbours(largestAdjListSize);
        }
        initNeighbours = new Neighbours();
        cachedNeighbours = new Neighbours(largestAdjListSize);
        if (ALDs.size() > 2) {
            tempNeighbours = new Neighbours(largestAdjListSize);
        }
    }

    public String getALDsAsString() {
        if (!DIFFERENTIATE_FWD_BWD_SINGLE_ALD && 1 == ALDs.size()) {
            return "E" + ALDs.get(0).getLabel();
        }
        String[] directions = new String[ALDs.size()];
        for (var i = 0; i < ALDs.size(); i++) {
            var label = ALDs.get(i).getLabel();
            directions[i] = (ALDs.get(i).getDirection() == Direction.Fwd ? "F" : "B") + label;
        }
        Arrays.sort(directions);
        return String.join("-", directions);
    }

    /**
     * @see Operator#hasMultiEdgeExtends()
     */
    @Override
    public boolean hasMultiEdgeExtends() {
        if (ALDs.size() > 1) {
            return true;
        }
        return prev.hasMultiEdgeExtends();
    }

    /**
     * Checks if the intersection is the same as the previous one.
     *
     * @return True if the intersection is the same as the previous one. Otherwise, return false.
     */
    protected boolean isIntersectionCached() {
        isIntersectionCached = true;
        for (int i = 0; i < lastVertexIdsIntersected.length; ++i) {
            if (lastVertexIdsIntersected[i] != probeTuple[vertexIdxToCache[i]]) {
                isIntersectionCached = false;
                lastVertexIdsIntersected[i] = probeTuple[vertexIdxToCache[i]];
            }
        }
        return isIntersectionCached;
    }

    private void setOperatorName() {
        var variables = new String[ALDs.size()];
        for (int i = 0; i < ALDs.size(); i++) {
            variables[i] = ALDs.get(i).getFromQueryVertex() + "[" +
                (ALDs.get(i).getDirection() == Direction.Fwd ? "Fwd" : "Bwd") + "]";
        }
        Arrays.sort(variables);
        StringJoiner joiner = new StringJoiner("-");
        for (var variable : variables) {
            joiner.add(variable);
        }
        if (1 == ALDs.size()) {
            name = "Single-Edge-Extend";
        } else {
            name = "Multi-Edge-Extend";
        }
        name += " TO (" + toQueryVertex + ") From (" + joiner.toString() + ")";
    }

    public void updateOperatorName(Map<String, Integer> queryVertexToIdxMap) {
        var prevToQueryVertices = new String[queryVertexToIdxMap.size()];
        for (var queryVertex : queryVertexToIdxMap.keySet()) {
            prevToQueryVertices[queryVertexToIdxMap.get(queryVertex)] = queryVertex;
        }
        name = Arrays.toString(prevToQueryVertices) + " - " + name;
        queryVertexToIdxMap.put(toQueryVertex, queryVertexToIdxMap.size());
        if (next != null) {
            for (var nextOperator : next) {
                nextOperator.updateOperatorName(MapUtils.copy(queryVertexToIdxMap));
            }
        }
    }
}
