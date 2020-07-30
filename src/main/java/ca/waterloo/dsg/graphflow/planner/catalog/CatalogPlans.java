package ca.waterloo.dsg.graphflow.planner.catalog;

import ca.waterloo.dsg.graphflow.plan.Plan;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanSampling;
import ca.waterloo.dsg.graphflow.planner.catalog.operator.IntersectCatalog;
import ca.waterloo.dsg.graphflow.planner.catalog.operator.Noop;
import ca.waterloo.dsg.graphflow.query.QueryEdge;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.query.QueryGraphSet;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.util.collection.SetUtils;
import ca.waterloo.dsg.graphflow.util.container.Triple;
import lombok.Getter;
import lombok.var;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Constructs a plan to collect stats (i-cost & cardinality) of operators with input subgraphs and
 * considering all combinations of possible ALDs accounting for edge labelsOrToTypes & types.
 * The stats collected are used to populate the {@link Catalog}.
 */
public class CatalogPlans {

    /**
     * An internal operator descriptor.
     */
    public static class Descriptor {

        Descriptor(QueryGraph outSubgraph, List<AdjListDescriptor> ALDs) {
            this.outSubgraph = outSubgraph;
            this.ALDs = ALDs;
        }

        public QueryGraph outSubgraph;
        public List<AdjListDescriptor> ALDs;
    }

    public static int DEF_NUM_EDGES_TO_SAMPLE = 1000;
    public static int DEF_MAX_INPUT_NUM_VERTICES = 3;
    private int numSampledEdges;
    private int maxInputNumVertices;

    private short numTypes;
    private short numLabels;
    private boolean isAdjListSortedByType;
    @Getter private QueryGraphSet queryGraphsToExtend = new QueryGraphSet();

    private static final String[] QUERY_VERTICES = { "a", "b", "c", "d", "e", "f", "g" };
    private static Map<String, Integer> QUERY_VERTEX_TO_IDX_MAP;
    @Getter private List<Plan[]> queryPlansArrs;
    private boolean isDirected;

    @Getter private List<Triple<QueryGraph, List<AdjListDescriptor>, Short>> selectivityZero;

    /**
     * Constructs a {@link CatalogPlans} object.
     *
     * @param graph is the input data graph.
     * @param store is the labels and types key store.
     * @param numThreads is the number of threads to use when executing.
     */
    CatalogPlans(Graph graph, KeyStore store, int numThreads, int numSampledEdges,
        int maxInputNumVertices) {
        this.maxInputNumVertices = maxInputNumVertices;
        this.numSampledEdges = numSampledEdges;
        if (null == QUERY_VERTEX_TO_IDX_MAP) {
            QUERY_VERTEX_TO_IDX_MAP = new HashMap<>();
            for (var i = 0; i < QUERY_VERTICES.length; i++) {
                QUERY_VERTEX_TO_IDX_MAP.put(QUERY_VERTICES[i], i);
            }
        }
        this.isDirected = !graph.isUndirected();
        this.numTypes = store.getNextTypeKey();
        this.numLabels = store.getNextLabelKey();
        this.isAdjListSortedByType = graph.isAdjListSortedByType();
        this.queryPlansArrs = new ArrayList<>();
        this.selectivityZero = new ArrayList<>();
        List<ScanSampling> scans;
        if (store.getNextLabelKey() == 1 && store.getNextTypeKey() == 1 &&
                graph.getNumEdges() > 1073741823) {
            scans = generateAllScansForLargeGraph(graph);
        } else {
            scans = generateAllScans(graph);
        }
        for (var scan : scans) {
            var noop = new Noop(scan.getOutSubgraph());
            scan.setNext(noop);
            noop.setPrev(scan);
            noop.setOutQVertexToIdxMap(scan.getOutQVertexToIdxMap());
            setNextOperators(graph, noop, queryGraphsToExtend);
            var queryPlansArr = new Plan[numThreads];
            queryPlansArr[0] = new Plan(scan);
            for (var i = 1; i < numThreads; i++) {
                var scanCopy = scan.copy();
                var anotherNoop = new Noop(scanCopy.getOutSubgraph());
                scanCopy.setNext(anotherNoop);
                anotherNoop.setPrev(scanCopy);
                anotherNoop.setOutQVertexToIdxMap(scanCopy.getOutQVertexToIdxMap());
                setNextOperators(graph, anotherNoop, null /* queryGraphExtendedFrom */);
                queryPlansArr[i] = new Plan(scanCopy);
            }
            queryPlansArrs.add(queryPlansArr);
        }
    }

    private void setNextOperators(Graph graph, Operator operator,
        QueryGraphSet queryGraphsToExtend) {
        var inSubgraph = operator.getOutSubgraph();
        if (null != queryGraphsToExtend && !queryGraphsToExtend.contains(inSubgraph)) {
            queryGraphsToExtend.add(inSubgraph);
        } else if (queryGraphsToExtend != null) {
            return;
        }

        var queryVertices = new ArrayList<String>(inSubgraph.getQVertices());
        var descriptors = new ArrayList<Descriptor>();
        for (var queryVerticesToExtend : SetUtils.getPowerSetExcludingEmptySet(queryVertices)) {
            for (var ALDs : generateALDs(queryVerticesToExtend, isDirected)) {
                descriptors.add(new Descriptor(getOutSubgraph(inSubgraph.copy(), ALDs), ALDs));
            }
        }
        var toQVertex = QUERY_VERTICES[inSubgraph.getNumVertices()];
        IntersectCatalog[] next;
        if (isAdjListSortedByType) {
            var nextList = new ArrayList<IntersectCatalog>();
            for (var descriptor : descriptors) {
                var types = new ArrayList<Short>();
                for (short toType = 0; toType < numTypes; toType++) {
                    boolean producesOutput = true;
                    for (var ALD : descriptor.ALDs) {
                        short fromType = inSubgraph.getVertexType(ALD.getFromQueryVertex());
                        if ((ALD.getDirection() == Direction.Fwd &&
                                0 == graph.getNumEdges(fromType, toType, ALD.getLabel())) ||
                            (ALD.getDirection() == Direction.Bwd &&
                                0 == graph.getNumEdges(toType, fromType, ALD.getLabel()))) {
                            producesOutput = false;
                            break;
                        }
                    }
                    if (producesOutput) {
                        types.add(toType);
                    } else {
                        selectivityZero.add(new Triple<>(inSubgraph, descriptor.ALDs, toType));
                    }
                }
                var outQVertexToIdxMap = new HashMap<>(operator.getOutQVertexToIdxMap());
                outQVertexToIdxMap.put(toQVertex, outQVertexToIdxMap.size());
                for (var toType : types) {
                    descriptor.outSubgraph.setVertexType(toQVertex, toType);
                    var intersect = new IntersectCatalog(toQVertex, toType, descriptor.ALDs,
                        descriptor.outSubgraph, inSubgraph, outQVertexToIdxMap,
                        isAdjListSortedByType);
                    intersect.initCaching(operator.getLastRepeatedVertexIdx());
                    nextList.add(intersect);
                }
            }
            next = nextList.toArray(new IntersectCatalog[0]);
        } else {
            next = new IntersectCatalog[descriptors.size()];
            for (var i = 0; i < descriptors.size(); i++) {
                var descriptor = descriptors.get(i);
                var outQVertexToIdxMap = new HashMap<>(operator.getPrev().getOutQVertexToIdxMap());
                outQVertexToIdxMap.put(toQVertex, outQVertexToIdxMap.size());
                next[i] = new IntersectCatalog(toQVertex, KeyStore.ANY, descriptor.ALDs, descriptor.
                    outSubgraph, inSubgraph, outQVertexToIdxMap, isAdjListSortedByType);
                next[i].initCaching(operator.getPrev().getLastRepeatedVertexIdx());
            }
        }
        setNextPointers(operator, next);
        for (var nextOperator : next) {
            var outSubgraph = nextOperator.getOutSubgraph();
            Noop[] nextNoops;
            if (isAdjListSortedByType) {
                nextNoops = new Noop[1];
            } else {
                nextNoops = new Noop[numTypes];
            }
            setNoops(outSubgraph, toQVertex, nextNoops, nextOperator.getOutQVertexToIdxMap());
            setNextPointers(nextOperator, nextNoops);
            if (outSubgraph.getNumVertices() <= maxInputNumVertices) {
                for (var nextNoop : nextNoops) {
                    nextNoop.setLastRepeatedVertexIdx(nextOperator.getLastRepeatedVertexIdx());
                    setNextOperators(graph, nextNoop, queryGraphsToExtend);
                }
            }
        }
    }

    private List<ScanSampling> generateAllScans(Graph graph) {
        var fwdAdjLists = graph.getFwdAdjLists();
        var vertexTypes = graph.getVertexTypes();
        var numVertices = graph.getHighestVertexId() + 1;
        var keyToEdgesMap = new HashMap<Long/*edge key*/, int[]/*edges*/ >();
        var keyToCurrIdx = new HashMap<Long/*edge key*/, Integer>();
        for (short fromType = 0; fromType < numTypes; fromType++) {
            for (short label = 0; label < numLabels; label++) {
                for (short toType = 0; toType < numTypes; toType++) {
                    var edgeKey = Graph.getEdgeKey(fromType, toType, label);
                    var numEdges = graph.getNumEdges(fromType, toType, label);
                    if (numEdges == 0) {
                        var x = 2;
                    }
                    keyToEdgesMap.put(edgeKey, new int[numEdges * 2]);
                    keyToCurrIdx.put(edgeKey, 0);
                }
            }
        }
        for (var fromVertex = 0; fromVertex < numVertices; fromVertex++) {
            var fromType = vertexTypes[fromVertex];
            var offsets = fwdAdjLists[fromVertex].getLabelOrTypeOffsets();
            var neighbours = fwdAdjLists[fromVertex].getNeighbourIds();
            for (short labelOrType = 0; labelOrType < offsets.length - 1; labelOrType++) {
                for (var toIdx = offsets[labelOrType]; toIdx < offsets[labelOrType + 1]; toIdx++) {
                    short toType, label;
                    if (isAdjListSortedByType) {
                        toType = labelOrType;
                        label = 0;
                    } else {
                        toType = vertexTypes[neighbours[toIdx]];
                        label = labelOrType;
                    }
                    var edgeKey = Graph.getEdgeKey(fromType, toType, label);
                    var currIdx = keyToCurrIdx.get(edgeKey);
                    keyToCurrIdx.put(edgeKey, currIdx + 2);
                    keyToEdgesMap.get(edgeKey)[currIdx] = fromVertex;
                    keyToEdgesMap.get(edgeKey)[currIdx + 1] = neighbours[toIdx];
                }
            }
        }
        var scans = new ArrayList<ScanSampling>();
        for (short fromType = 0; fromType < numTypes; fromType++) {
            for (short label = 0; label < numLabels; label++) {
                for (short toType = 0; toType < numTypes; toType++) {
                    var outSubgraph = new QueryGraph();
                    outSubgraph.addEdge(new QueryEdge("a", "b", fromType, toType, label));
                    var edgeKey = Graph.getEdgeKey(fromType, toType, label);
                    var actualNumEdges = graph.getNumEdges(fromType, toType, label);
                    if (actualNumEdges > 0) {
                        var numEdgesToSample = (int) (numSampledEdges * (
                            graph.getNumEdges(fromType, toType, label) /
                                (double) graph.getNumEdges()));
                        var scan = new ScanSampling(outSubgraph);
                        if (isAdjListSortedByType && numEdgesToSample < 1000) {
                            numEdgesToSample = actualNumEdges;
                        }
                        scan.setEdgeIndicesToSample(keyToEdgesMap.get(edgeKey), numEdgesToSample);
                        scans.add(scan);
                    }
                }
            }
        }
        return scans;
    }

    private List<ScanSampling> generateAllScansForLargeGraph(Graph graph) {
        var fwdAdjLists = graph.getFwdAdjLists();
        var numVertices = graph.getHighestVertexId() + 1;
        var edges = new ArrayList<int[]>();
        for (var fromVertex = 0; fromVertex < numVertices; fromVertex++) {
            for (var toVertex : fwdAdjLists[fromVertex].getNeighbourIds()) {
                edges.add(new int[] { fromVertex, toVertex });
            }
        }
        var outSubgraph = new QueryGraph();
        outSubgraph.addEdge(new QueryEdge("a", "b", (short) 0, (short) 0, (short) 0));
        var scan = new ScanSampling(outSubgraph);
        scan.setEdgeIndicesToSample(edges, numSampledEdges);
        var scans = new ArrayList<ScanSampling>();
        scans.add(scan);
        return scans;
    }

    private void setNextPointers(Operator operator, Operator[] next) {
        operator.setNext(next);
        for (var nextOperator : next) {
            nextOperator.setPrev(operator);
        }
    }

    private void setNoops(QueryGraph queryGraph, String toQVertex, Noop[] Noops,
        Map<String, Integer> outQVertexToIdxMap) {
        if (isAdjListSortedByType) {
            Noops[0] = new Noop(queryGraph);
            Noops[0].setOutQVertexToIdxMap(outQVertexToIdxMap);
        } else {
            for (short toType = 0; toType < numTypes; toType++) {
                var queryGraphCopy = queryGraph.copy();
                queryGraphCopy.setVertexType(toQVertex, toType);
                Noops[toType] = new Noop(queryGraphCopy);
            }
        }
    }

    private List<List<AdjListDescriptor>> generateALDs(List<String> qVertices, boolean isDirected) {
        var directionPatterns = generateDirectionPatterns(qVertices.size(), isDirected);
        var labelPatterns = generateLabelsPatterns(qVertices.size());
        var ALDsList = new ArrayList<List<AdjListDescriptor>>();
        for (var directions : directionPatterns) {
            for (var labels : labelPatterns) {
                var ALDs = new ArrayList<AdjListDescriptor>();
                for (int i = 0; i < directions.length; i++) {
                    var vertexIdx = QUERY_VERTEX_TO_IDX_MAP.get(qVertices.get(i));
                    var toQVertex = QUERY_VERTICES[vertexIdx];
                    var label = labels.get(i);
                    ALDs.add(new AdjListDescriptor(toQVertex, vertexIdx, directions[i], label));
                }
                ALDsList.add(ALDs);
            }
        }
        return ALDsList;
    }

    private QueryGraph getOutSubgraph(QueryGraph queryGraph, List<AdjListDescriptor> ALDs) {
        var numQVertices = queryGraph.getNumVertices();
        for (var ALD : ALDs) {
            QueryEdge queryEdge;
            if (ALD.getDirection() == Direction.Fwd) {
                queryEdge = new QueryEdge(ALD.getFromQueryVertex(), QUERY_VERTICES[numQVertices]);
                queryEdge.setFromType(queryGraph.getVertexType(ALD.getFromQueryVertex()));
            } else { // ALD.getDirection() == Direction.Bwd
                queryEdge = new QueryEdge(QUERY_VERTICES[numQVertices], ALD.getFromQueryVertex());
                queryEdge.setToType(queryGraph.getVertexType(ALD.getFromQueryVertex()));
            }
            queryEdge.setLabel(ALD.getLabel());
            queryGraph.addEdge(queryEdge);
        }
        return queryGraph;
    }

    private List<List<Short>> generateLabelsPatterns(int size) {
        var labels = new ArrayList<Short>();
        for (short label = 0; label < numLabels; label++) {
            labels.add(label);
        }
        return new ArrayList<>(SetUtils.generatePermutations(labels, size));
    }

    public static List<Direction[]> generateDirectionPatterns(int size, boolean isDirected) {
        var directionPatterns = new ArrayList<Direction[]>();
        generateDirectionPatterns(new Direction[size], size, directionPatterns, isDirected);
        return directionPatterns;
    }

    private static void generateDirectionPatterns(Direction[] directionArr, int size,
        List<Direction[]> directionPatterns, boolean isDirected) {
        if (size <= 0) {
            directionPatterns.add(Arrays.copyOf(directionArr, directionArr.length));
        } else {
            directionArr[size - 1] = Direction.Bwd;
            generateDirectionPatterns(directionArr, size - 1, directionPatterns, isDirected);
            if (isDirected) {
                directionArr[size - 1] = Direction.Fwd;
                generateDirectionPatterns(directionArr, size - 1, directionPatterns, isDirected);
            }
        }
    }
}
