package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.plan.Plan;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.EI;
import ca.waterloo.dsg.graphflow.plan.operator.extend.EI.CachingType;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.HashJoin;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink.SinkType;
import ca.waterloo.dsg.graphflow.planner.catalog.Catalog;
import ca.waterloo.dsg.graphflow.query.QueryEdge;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.util.collection.SetUtils;
import lombok.var;
import org.antlr.v4.runtime.misc.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Generates a {@link Plan}. The intersection cost (ICost) is used as a metric of the
 * optimization.
 */
public class QueryPlanner {

    private Map<Integer /* level: #(qVertices) covered [2, n] where n = #vertices in the query */,
        Map<String /* hash for query vertices covered */, List<Plan>>> subgraphPlans;

    QueryGraph queryGraph;
    int numVertices;
    int nextNumQVertices;
    protected Graph graph;
    protected Catalog catalog;
    boolean hasLimit;

    private int nextHashJoinID = 0;

    private Map<String /* encoding */,
                List<Pair<QueryGraph /*subgraph*/, Double /*selectivity*/>>> computedSelectivity;

    /**
     * Constructs a {@link QueryPlanner} object.
     *
     * @param queryGraph is the {@link QueryGraph} to evaluate.
     * @param catalog is the catalog containing cost and selectivity stats of and intersections
     * for the graph.
     * @param graph is the graph to evaluate the query on.
     */
    public QueryPlanner(QueryGraph queryGraph, Catalog catalog, Graph graph) {
        this.queryGraph = queryGraph;
        this.hasLimit = queryGraph.getLimit() > 0;
        this.catalog = catalog;
        this.graph = graph;
        this.subgraphPlans = new HashMap<>();
        this.numVertices = queryGraph.getNumVertices();
        this.computedSelectivity = new HashMap<>(1000 /*capacity*/);
    }

    /**
     * Returns based on the optimizer the 'best' {@link Plan} to evaluate a given
     * {@link QueryGraph}.
     *
     * @return The generated {@link Plan} to evaluate the input query graph.
     */
    public Plan plan() {
        if (numVertices == 2) {
            return new Plan(new Scan(queryGraph));
        }
        considerAllScanOperators();
        while (nextNumQVertices <= numVertices) {
            considerAllNextQueryExtensions();
            nextNumQVertices++;
        }
        var key = subgraphPlans.get(numVertices).keySet().iterator().next();
        var bestPlan = getBestPlan(numVertices, key);
        // each operator added only sets its prev pointer (to reuse operator objects).
        // the picked plan needs to set the next pointer for each operator in the linear subplans.
        setNextPointers(bestPlan);
        if (hasLimit) {
            bestPlan.setSinkType(SinkType.LIMIT);
            bestPlan.setOutTuplesLimit(queryGraph.getLimit());
        }
        return bestPlan;
    }

    void setNextPointers(Plan bestPlan) {
        for (var lastOperator : bestPlan.getSubplans()) {
            var operator = lastOperator;
            while (null != operator.getPrev()) {
                operator.getPrev().setNext(operator);
                operator = operator.getPrev();
            }
        }
    }

    private void considerAllScanOperators() {
        nextNumQVertices = 2; /* level = 2 for edge scan */
        subgraphPlans.putIfAbsent(nextNumQVertices, new HashMap<>());
        for (var queryEdge : queryGraph.getEdges()) {
            var outSubgraph = new QueryGraph();
            outSubgraph.addEdge(queryEdge);
            var scan = new Scan(outSubgraph);
            var numEdges = getNumEdges(queryEdge);
            var queryPlan = new Plan(scan, numEdges);
            var queryPlans = new ArrayList<Plan>();
            queryPlans.add(queryPlan);
            subgraphPlans.get(nextNumQVertices).put(getKey(new String[] {
                queryEdge.getFromVertex(), queryEdge.getToVertex() }), queryPlans);
        }
        nextNumQVertices = 3;
    }

    private void considerAllNextQueryExtensions() {
        subgraphPlans.putIfAbsent(nextNumQVertices, new HashMap<>());
        var prevNumQVertices = nextNumQVertices - 1;
        for(var prevQueryPlans : subgraphPlans.get(prevNumQVertices).values()) {
            considerAllNextExtendOperators(prevQueryPlans);
        }
        if (!hasLimit && nextNumQVertices >= 4) {
            for (var queryPlans : subgraphPlans.get(nextNumQVertices).values()) {
                var outSubgraph = queryPlans.get(0).getLastOperator().getOutSubgraph();
                considerAllNextHashJoinOperators(outSubgraph);
            }
        }
    }

    private void considerAllNextExtendOperators(List<Plan> prevQueryPlans) {
        var prevQVertices = prevQueryPlans.get(0).getLastOperator().getOutSubgraph().getQVertices();
        var toQVertices = queryGraph.getNeighbors(new HashSet<>(prevQVertices));
        for (String toQVertex : toQVertices) {
            for (var prevQueryPlan : prevQueryPlans) {
                Pair<String /* key */, Plan> newQueryPlan = getPlanWithNextExtend(
                    prevQueryPlan, toQVertex);
                subgraphPlans.get(nextNumQVertices).putIfAbsent(newQueryPlan.a, new ArrayList<>());
                subgraphPlans.get(nextNumQVertices).get(newQueryPlan.a).add(newQueryPlan.b);
            }
        }
    }

    Pair<String, Plan> getPlanWithNextExtend(Plan prevQueryPlan, String toQVertex) {
        var lastOperator = prevQueryPlan.getLastOperator();
        var inSubgraph = lastOperator.getOutSubgraph();
        var ALDs = new ArrayList<AdjListDescriptor>();
        var toType = queryGraph.getVertexType(toQVertex);
        var nextExtend = getNextEI(inSubgraph, toQVertex, ALDs, lastOperator);
        var lastPreviousRepeatedIndex = lastOperator.getLastRepeatedVertexIdx();
        nextExtend.initCaching(lastPreviousRepeatedIndex);

        var prevEstimatedNumOutTuples = prevQueryPlan.getEstimatedNumOutTuples();
        var estimatedSelectivity = getSelectivity(inSubgraph, nextExtend.getOutSubgraph(), ALDs,
            nextExtend.getOutSubgraph().getVertexType(toQVertex));
        double icost;
        if (nextExtend.getCachingType() == CachingType.NONE) {
            icost = prevEstimatedNumOutTuples * catalog.getICost(inSubgraph, ALDs,
                nextExtend.getToType());
        } else {
            var outTuplesToProcess = prevEstimatedNumOutTuples;
            if (null != prevQueryPlan.getLastOperator().getPrev()) {
                var index = -1;
                var lastEstimatedNumOutTuplesForExtensionQVertex = -1.0;
                for (var ALD : ALDs) {
                    if (ALD.getVertexIdx() > index) {
                        lastEstimatedNumOutTuplesForExtensionQVertex = prevQueryPlan.
                            getQVertexToNumOutTuples().get(ALD.getFromQueryVertex());
                    }
                }
                outTuplesToProcess /= lastEstimatedNumOutTuplesForExtensionQVertex;
            }
            if (nextExtend.getCachingType() == CachingType.FULL_CACHING) {
                icost = outTuplesToProcess * catalog.getICost(inSubgraph, ALDs, toType) +
                    // added to make caching effect on cost more robust.
                    (prevEstimatedNumOutTuples - outTuplesToProcess) * estimatedSelectivity;
            } else { // cachingType == CachingType.PARTIAL_CACHING
                var ALDsToCache = ALDs.stream()
                    .filter(ALD -> ALD.getVertexIdx() <= lastPreviousRepeatedIndex)
                    .collect(Collectors.toList());
                var ALDsToAlwaysIntersect = ALDs.stream()
                    .filter(ALD -> ALD.getVertexIdx() > lastPreviousRepeatedIndex)
                    .collect(Collectors.toList());
                var alwaysIntersectICost = prevEstimatedNumOutTuples * catalog.getICost(
                    inSubgraph, ALDsToAlwaysIntersect, toType);
                var cachedIntersectICost = outTuplesToProcess * catalog.getICost(
                    inSubgraph, ALDsToCache, toType);
                icost = prevEstimatedNumOutTuples * alwaysIntersectICost +
                    outTuplesToProcess * cachedIntersectICost +
                    // added to make caching effect on cost more robust.
                    (prevEstimatedNumOutTuples - outTuplesToProcess) * estimatedSelectivity;
            }
        }

        var estimatedICost = prevQueryPlan.getEstimatedICost() + icost;
        var estimatedNumOutTuples = prevEstimatedNumOutTuples * estimatedSelectivity;

        var qVertexToNumOutTuples = new HashMap<String, Double>();
        qVertexToNumOutTuples.putAll(prevQueryPlan.getQVertexToNumOutTuples());
        qVertexToNumOutTuples.put(nextExtend.getToQueryVertex(), estimatedNumOutTuples);

        var newQueryPlan = prevQueryPlan.shallowCopy();
        newQueryPlan.setEstimatedICost(estimatedICost);
        newQueryPlan.setEstimatedNumOutTuples(estimatedNumOutTuples);
        newQueryPlan.append(nextExtend);
        newQueryPlan.setQVertexToNumOutTuples(qVertexToNumOutTuples);
        return new Pair<>(getKey(nextExtend.getOutQVertexToIdxMap().keySet()), newQueryPlan);
    }

    private EI getNextEI(QueryGraph inSubgraph, String toQVertex, List<AdjListDescriptor> ALDs,
        Operator lastOperator) {
        var outSubgraph = inSubgraph.copy();
        for (String fromQVertex : inSubgraph.getQVertices()) {
            if (queryGraph.containsQueryEdge(fromQVertex, toQVertex)) {
                // simple query graph so there is only 1 queryEdge, so get queryEdge at index '0'.
                var queryEdge = queryGraph.getEdge(fromQVertex, toQVertex);
                var index = lastOperator.getOutQVertexToIdxMap().get(fromQVertex);
                var direction = fromQVertex.equals(queryEdge.getFromVertex()) ?
                    Direction.Fwd : Direction.Bwd;
                var label = queryEdge.getLabel();
                ALDs.add(new AdjListDescriptor(fromQVertex, index, direction, label));
                outSubgraph.addEdge(queryEdge);
            }
        }
        var outputVariableIdxMap = new HashMap<String, Integer>();
        outputVariableIdxMap.putAll(lastOperator.getOutQVertexToIdxMap());
        outputVariableIdxMap.put(toQVertex, outputVariableIdxMap.size());
        return EI.make(toQVertex, queryGraph.getVertexType(toQVertex), ALDs, outSubgraph,
            inSubgraph, outputVariableIdxMap);
    }

    int getNumEdges(QueryEdge queryEdge) {
        var fromType = queryGraph.getVertexType(queryEdge.getFromVertex());
        var toType = queryGraph.getVertexType(queryEdge.getToVertex());
        var label = queryEdge.getLabel();
        return graph.getNumEdges(fromType, toType, label);
    }

    private double getSelectivity(QueryGraph inSubgraph, QueryGraph outSubgraph,
        List<AdjListDescriptor> ALDs, short toType) {
        double selectivity;

        if (computedSelectivity.containsKey(outSubgraph.getEncoding())) {
            for (var computedSelectivity : computedSelectivity.get(outSubgraph.getEncoding())) {
                if (computedSelectivity.a /* query graph */.isIsomorphicTo(outSubgraph)) {
                    return computedSelectivity.b;
                }
            }
        } else {
            computedSelectivity.put(outSubgraph.getEncoding(), new ArrayList<>());
        }
        selectivity = catalog.getSelectivity(inSubgraph, ALDs, toType);
        computedSelectivity.get(outSubgraph.getEncoding()).add(
            new Pair<>(outSubgraph, selectivity));
        return selectivity;
    }

    private void considerAllNextHashJoinOperators(QueryGraph outSubgraph) {
        var queryVertices = new ArrayList<>(outSubgraph.getQVertices());
        var minSize = 3;
        var maxSize = outSubgraph.getQVertices().size() - minSize;
        if (maxSize < minSize) {
            maxSize = minSize;
        }
        var it = IntStream.rangeClosed(minSize, maxSize).iterator();
        while (it.hasNext()) {
            var setSize = it.next();
            for (var key : subgraphPlans.get(setSize).keySet()) {
                var prevQueryPlan = getBestPlan(setSize, key);
                var prevQVertices = prevQueryPlan.getLastOperator().getOutSubgraph().getQVertices();
                if (SetUtils.isSubset(queryVertices, prevQVertices)) {
                    var otherSet = SetUtils.subtract(queryVertices, prevQVertices);
                    if (otherSet.size() == 1) {
                        continue;
                    }
                    var joinQVertices = getJoinQVertices(outSubgraph, prevQVertices, otherSet);
                    if (joinQVertices.size() != 1 ||
                            otherSet.size() + joinQVertices.size() > nextNumQVertices - 1) {
                        continue;
                    }
                    otherSet.addAll(joinQVertices);
                    var restKey = getKey(otherSet);
                    var restSize = otherSet.size();
                    if (subgraphPlans.get(restSize).containsKey(restKey)) {
                        var otherPrevOperator = getBestPlan(restSize, restKey);
                        considerHashJoinOperator(outSubgraph, queryVertices, prevQueryPlan,
                            otherPrevOperator, joinQVertices.size());
                    }
                }
            }
        }
    }

    private static List<String> getJoinQVertices(QueryGraph queryGraph, Collection<String> vertices,
        List<String> otherVertices) {
        var joinQVertices = new HashSet<String>();
        for (var vertex : vertices) {
            for (var otherVertex : otherVertices) {
                if (queryGraph.containsQueryEdge(vertex, otherVertex)) {
                    joinQVertices.add(vertex);
                }
            }
        }
        return new ArrayList<>(joinQVertices);
    }


    private void considerHashJoinOperator(QueryGraph outSubgraph, List<String> queryVertices,
        Plan subplan, Plan otherSubplan, int numJoinQVertices) {
        var isPlanBuildSubplan =
            subplan.getEstimatedNumOutTuples() < otherSubplan.getEstimatedNumOutTuples();
        var buildSubplan = isPlanBuildSubplan ? subplan : otherSubplan;
        var probeSubplan = isPlanBuildSubplan ? otherSubplan : subplan;
        var buildCoef = numJoinQVertices == 1 ?
            Catalog.SINGLE_VERTEX_WEIGHT_BUILD_COEF : Catalog.MULTI_VERTEX_WEIGHT_BUILD_COEF;
        var probeCoef = numJoinQVertices == 1 ?
            Catalog.SINGLE_VERTEX_WEIGHT_PROBE_COEF : Catalog.MULTI_VERTEX_WEIGHT_PROBE_COEF;
        var icost = buildSubplan.getEstimatedICost() + probeSubplan.getEstimatedICost() +
                buildCoef * buildSubplan.getEstimatedNumOutTuples() +
                probeCoef * probeSubplan.getEstimatedNumOutTuples();

        var key = getKey(queryVertices);
        var currBestQueryPlan = getBestPlan(queryVertices.size(), key);
        if (currBestQueryPlan.getEstimatedICost() > icost) {
            var queryPlan = HashJoin.make(outSubgraph, buildSubplan, probeSubplan,
                nextHashJoinID++);
            queryPlan.setEstimatedICost(icost);
            queryPlan.setEstimatedNumOutTuples(currBestQueryPlan.getEstimatedNumOutTuples());
            var vertexToNumOutTuples = new HashMap<String, Double>(
                probeSubplan.getQVertexToNumOutTuples());
            for (var vertex : buildSubplan.getLastOperator().getOutSubgraph().getQVertices()) {
                var estimatedNumOutTuples = currBestQueryPlan.getEstimatedNumOutTuples();
                vertexToNumOutTuples.putIfAbsent(vertex, estimatedNumOutTuples);
            }
            queryPlan.setQVertexToNumOutTuples(vertexToNumOutTuples);

            var queryPlans = subgraphPlans.get(queryVertices.size()).get(key);
            queryPlans.clear();
            queryPlans.add(queryPlan);
        }
    }

    private Plan getBestPlan(int numQVertices, String key) {
        var possibleQueryPlans = subgraphPlans.get(numQVertices).get(key);
        var bestPlan = possibleQueryPlans.get(0);
        for (var possibleQueryPlan : possibleQueryPlans) {
            if (possibleQueryPlan.getEstimatedICost() < bestPlan.getEstimatedICost()) {
                bestPlan = possibleQueryPlan;
            }
        }
        return bestPlan;
    }

    private String getKey(Collection<String> queryVertices) {
        var queryVerticesArr = new String[queryVertices.size()];
        queryVertices.toArray(queryVerticesArr);
        return getKey(queryVerticesArr);
    }

    private String getKey(String[] queryVertices) {
        Arrays.sort(queryVertices);
        return Arrays.toString(queryVertices);
    }
}
