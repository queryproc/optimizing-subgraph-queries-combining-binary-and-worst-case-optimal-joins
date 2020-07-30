package ca.waterloo.dsg.graphflow.planner;

import ca.waterloo.dsg.graphflow.plan.Plan;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink.SinkType;
import ca.waterloo.dsg.graphflow.planner.catalog.Catalog;
import ca.waterloo.dsg.graphflow.query.QueryEdge;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import lombok.var;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates a {@link Plan}. The intersection cost (ICost) is used as a metric of the
 * optimization and multiple heuristics are used to reduce the search space:
 * (1) Does not consider hash joins.
 * (2) Considered a sample equal to 10 of the edges to scan. The ones with least selectivity.
 * (3) Considers the next possible extension for query vertices with highest number of ALDs.
 * (4) Does not consider 'interesting orders' for better possible caching.
 */
public class QueryPlannerBig extends QueryPlanner {

    public static int NUM_TOP_PLANS_KEPT = 5;

    private Map<Integer /* level: #(qVertices) covered [2, n] where n = #vertices in the query */,
                List<Plan>> subgraphPlans;

    /**
     * @see QueryPlanner#QueryPlanner(QueryGraph, Catalog, Graph)
     */
    public QueryPlannerBig(QueryGraph queryGraph, Catalog catalog, Graph graph) {
        super(queryGraph, catalog, graph);
        subgraphPlans = new HashMap<>();
        if (numVertices >= 15) {
            NUM_TOP_PLANS_KEPT = 3;
        }
    }

    /**
     * Returns based on the optimizer the 'best' {@link Plan} to evaluate a given
     * {@link QueryGraph}.
     *
     * @return The generated {@link Plan} to evaluate the input query graph.
     */
    @Override
    public Plan plan() {
        considerLeastSelectiveScans();
        while (nextNumQVertices <= numVertices) {
            considerNextQueryExtensions();
            nextNumQVertices++;
        }
        var bestPlan = subgraphPlans.get(numVertices).get(0);
        for (var i = 1; i < subgraphPlans.get(numVertices).size(); i++) {
            var plan = subgraphPlans.get(numVertices).get(1);
            if (bestPlan.getEstimatedICost() > plan.getEstimatedICost()) {
                bestPlan = plan;
            }
        }
        // each operator added only sets its prev pointer (to reuse operator objects).
        // the picked plan needs to set the next pointer for each operator in the linear subplans.
        setNextPointers(bestPlan);
        if (queryGraph.getLimit() > 0) {
            bestPlan.setSinkType(SinkType.LIMIT);
            bestPlan.setOutTuplesLimit(queryGraph.getLimit());
        }
        return bestPlan;
    }

    private void considerLeastSelectiveScans() {
        nextNumQVertices = 2; /* level = 2 for edge scan */
        subgraphPlans.putIfAbsent(nextNumQVertices, new ArrayList<>(NUM_TOP_PLANS_KEPT));
        var edgesToScan = new QueryEdge[NUM_TOP_PLANS_KEPT];
        var numEdgesToScan = new int[NUM_TOP_PLANS_KEPT];
        var qEdges = queryGraph.getEdges();
        for (var i = 0; i < NUM_TOP_PLANS_KEPT; i++) {

            edgesToScan[i] = qEdges.get(i);
            numEdgesToScan[i] = getNumEdges(qEdges.get(i));
        }
        outer: for (var i = NUM_TOP_PLANS_KEPT; i < qEdges.size(); i++) {
            var numEdges = getNumEdges(qEdges.get(i));
            for (var j = 0; j < NUM_TOP_PLANS_KEPT; j++) {
                if (numEdges < numEdgesToScan[j]) {
                    edgesToScan[j] = qEdges.get(i);
                    numEdgesToScan[j] = numEdges;
                    continue outer;
                }
            }
        }
        for (var i = 0; i < NUM_TOP_PLANS_KEPT; i++) {
            var outputSubgraph = new QueryGraph();
            outputSubgraph.addEdge(edgesToScan[i]);
            var scan = new Scan(outputSubgraph);
            var queryPlan = new Plan(scan, numEdgesToScan[i]);
            subgraphPlans.get(nextNumQVertices).add(queryPlan);
        }
        nextNumQVertices = 3;
    }

    private void considerNextQueryExtensions() {
        var prevNumQVertices = nextNumQVertices - 1;
        var newQueryPlans = new ArrayList<Plan>(NUM_TOP_PLANS_KEPT);
        for (var prevQueryPlan : subgraphPlans.get(prevNumQVertices)) {
            var prevQVertices = prevQueryPlan.getLastOperator().getOutSubgraph().getQVertices();
            var toQVertices = queryGraph.getNeighbors(new HashSet<>(prevQVertices));
            var inSubgraph = prevQueryPlan.getLastOperator().getOutSubgraph();
            var nextToQVertices = filterToQVerticesByMaxNumALDs(toQVertices, inSubgraph);
            for (var toQVertex : nextToQVertices) {
                var keyAndPlan = getPlanWithNextExtend(prevQueryPlan, toQVertex);
                var icost = keyAndPlan.b.getEstimatedICost();
                if (newQueryPlans.size() < NUM_TOP_PLANS_KEPT) {
                    newQueryPlans.add(keyAndPlan.b);
                } else {
                    for (int i = 0; i < NUM_TOP_PLANS_KEPT; i++) {
                        if (newQueryPlans.get(i).getEstimatedICost() > icost) {
                            newQueryPlans.set(i, keyAndPlan.b);
                        }
                    }
                }
            }
        }
        subgraphPlans.put(nextNumQVertices, newQueryPlans);
        if (!hasLimit && nextNumQVertices >= 4) {
            // TODO: is this necessary?!
            for (var queryPlans : subgraphPlans.get(nextNumQVertices)) {
                var outSubgraph = queryPlans.getLastOperator().getOutSubgraph();
                // considerAllNextHashJoinOperators(outSubgraph);
            }
        }
    }

    private Set<String> filterToQVerticesByMaxNumALDs(Set<String> toQVertices,
        QueryGraph inSubgraph) {
        var maxNumALDs = Integer.MIN_VALUE;
        Map<String, Integer> toQVertexToNumALDsMap = new HashMap<>();
        for (var toQVertex : toQVertices) {
            var numALDs = 0;
            for (var fromQVertex : inSubgraph.getQVertices()) {
                if (queryGraph.containsQueryEdge(fromQVertex, toQVertex)) {
                    numALDs++;
                }
            }
            if (maxNumALDs < numALDs) {
                maxNumALDs = numALDs;
            }
            toQVertexToNumALDsMap.put(toQVertex, numALDs);
        }
        var finalMaxNumALDs = maxNumALDs;
        return toQVertices.
            stream().
            filter(toQVertex -> toQVertexToNumALDsMap.get(toQVertex) == finalMaxNumALDs).
            collect(Collectors.toSet());
    }
}
