package ca.waterloo.dsg.graphflow.planner.catalog;

import ca.waterloo.dsg.graphflow.plan.Plan;
import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.Operator.LimitExceededException;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanSampling;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink;
import ca.waterloo.dsg.graphflow.planner.catalog.operator.IntersectCatalog;
import ca.waterloo.dsg.graphflow.planner.catalog.operator.Noop;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.query.QueryGraphSet;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.storage.SortedAdjList;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Computes and holds stats about cost of intersections given labels & types for the current graph.
 */
public class Catalog {

    private static final Logger logger = LogManager.getLogger(Catalog.class);

    // These are calibrated values obtained through experimentation.
    public static double SINGLE_VERTEX_WEIGHT_PROBE_COEF =   3.0;
    public static double SINGLE_VERTEX_WEIGHT_BUILD_COEF =  12.0;
    public static double MULTI_VERTEX_WEIGHT_PROBE_COEF  =  12.0;
    public static double MULTI_VERTEX_WEIGHT_BUILD_COEF  = 720.0;

    private List<QueryGraph> inSubgraphs;
    private Map<Integer/*inSubgraph idx*/, Map<String/*ALD (toType?)*/,Double>> sampledIcost;
    private Map<Integer/*inSubgraph idx*/, Map<String/*ALDs & toType*/,Double>> sampledSelectivity;
    @Setter private boolean isAdjListSortedByType;
    @Setter private int numSampledEdges;
    @Setter private int maxInputNumVertices;

    @Setter protected double elapsedTime = 0;

    /**
     * Constructs a {@link Catalog} object.
     */
    public Catalog(int numSampledEdges, int maxInputNumVertices) {
        this.numSampledEdges = numSampledEdges;
        this.maxInputNumVertices = maxInputNumVertices;
    }

    /**
     * Constructs a {@link Catalog} object.
     *
     * @param icost is the intersection costs sampled.
     * @param cardinality is the sampledSelectivity sampled.
     * @param inSubgraphs are the set of input subgraphs sampled.
     */
    public Catalog(Map<Integer, Map<String, Double>> icost,
        Map<Integer, Map<String, Double>> cardinality, List<QueryGraph> inSubgraphs) {
        this.sampledIcost = icost;
        this.sampledSelectivity = cardinality;
        this.inSubgraphs = inSubgraphs;
    }

    /**
     * Returns the i-cost of a particular extension from an input {@link QueryGraph}.
     *
     * @param queryGraph is the {@link QueryGraph} to extend from.
     * @param ALDs are the {@link AdjListDescriptor}s of the extension.
     * @param toType is the to query vertex type.
     * @return the intersection cost of extending from the inSubgraph given the ALDs.
     */
    public double getICost(QueryGraph queryGraph, List<AdjListDescriptor> ALDs, short toType) {
        var approxIcost = 0.0;
        var minICost = Double.MAX_VALUE;
        for (var ALD : ALDs) {
            // Get each ALD icost by finding the largest subgraph (num vertices then num edges)
            // of queryGraph used in stats collection and also minimizing sampledIcost.
            var numVertices = CatalogPlans.DEF_MAX_INPUT_NUM_VERTICES - 1;
            while (numVertices >= 2) {
                minICost = Double.MAX_VALUE;
                var numEdgesMatched = 0;
                for (var i = 0; i < inSubgraphs.size(); i++) {
                    if (inSubgraphs.get(i).getNumVertices() != numVertices) {
                        continue;
                    }
                    var it = queryGraph.getSubgraphMappingIterator(inSubgraphs.get(i));
                    var newNumEdgesMatched = inSubgraphs.get(i).getEdges().size();
                    if (newNumEdgesMatched < numEdgesMatched) {
                        continue;
                    }
                    while (it.hasNext()) {
                        var newVertexMapping = it.next();
                        if (null == newVertexMapping.get(ALD.getFromQueryVertex())) {
                            continue;
                        }
                        double sampledIcost;
                        var ALDasStr = "(" + newVertexMapping.get(ALD.getFromQueryVertex()) + ") " +
                            ALD.getDirection().name() + "[" + ALD.getLabel() + "]";
                        if (isAdjListSortedByType) {
                            sampledIcost = this.sampledSelectivity.get(i).get(ALDasStr + "~" +
                                toType);
                        } else {
                            sampledIcost = this.sampledIcost.get(i).get(ALDasStr);
                        }
                        if (newNumEdgesMatched > numEdgesMatched || minICost > sampledIcost) {
                            minICost = sampledIcost;
                            numEdgesMatched = newNumEdgesMatched;
                        }
                    }
                }
                if (minICost < Double.MAX_VALUE) {
                    break;
                }
                numVertices--;
            }
            approxIcost += minICost;
        }
        return approxIcost;
    }

    /**
     * Returns the sampledSelectivity of a particular extension from an input {@link QueryGraph}.
     *
     * @param inSubgraph is the {@link QueryGraph} to extend from.
     * @param ALDs are the {@link AdjListDescriptor}s of the extension.
     * @param toType is the to query vertex type.
     * @return the intersection cost of extending from the inSubgraph given the ALDs.
     */
    public double getSelectivity(QueryGraph inSubgraph, List<AdjListDescriptor> ALDs,
        short toType) {
        var approxSelectivity = Double.MAX_VALUE;
        var numVertices = CatalogPlans.DEF_MAX_INPUT_NUM_VERTICES - 1;
        while (numVertices >= 2) {
            var numALDsMatched = 0;
            for (var i = 0; i < inSubgraphs.size(); i++) {
                if (inSubgraphs.get(i).getNumVertices() != numVertices) {
                    continue;
                }
                var it = inSubgraph.getSubgraphMappingIterator(inSubgraphs.get(i));
                while (it.hasNext()) {
                    var vertexMapping = it.next();
                    int newNumALDsMatched = getNumALDsMatched(ALDs, vertexMapping);
                    if (newNumALDsMatched == 0 || newNumALDsMatched < numALDsMatched) {
                        continue;
                    }
                    var strValue = getALDsAsStr(ALDs, vertexMapping, toType);
                    var sampledSelectivity = this.sampledSelectivity.get(i).get(strValue);
                    if (newNumALDsMatched > numALDsMatched ||
                            sampledSelectivity < approxSelectivity) {
                        numALDsMatched = newNumALDsMatched;
                        approxSelectivity = sampledSelectivity;
                    }
                }
            }
            numVertices--;
        }
        return approxSelectivity;
    }

    private String getALDsAsStr(List<AdjListDescriptor> ALDs, Map<String, String> vertexMapping,
        Short toType) {
        var fromQVerticesAndDirs = new ArrayList<String>();
        if (vertexMapping == null) {
            for (var ALD : ALDs) {
                fromQVerticesAndDirs.add("(" + ALD.getFromQueryVertex() + ") " +
                    ALD.getDirection().name() + "[" + ALD.getLabel() + "]");
            }
        } else {
            for (var ALD : ALDs) {
                if (null != vertexMapping.get(ALD.getFromQueryVertex())) {
                    fromQVerticesAndDirs.add("(" + vertexMapping.get(ALD.getFromQueryVertex()) +
                        ") " + ALD.getDirection().name() + "[" + ALD.getLabel() + "]");
                }
            }
        }
        java.util.Collections.sort(fromQVerticesAndDirs);
        var builder = new StringBuilder();
        for (int i = 0; i < fromQVerticesAndDirs.size(); i++) {
            builder.append(fromQVerticesAndDirs.get(i));
            if (i < fromQVerticesAndDirs.size() - 1) {
                builder.append(", ");
            }
        }
        return builder.toString() + (toType != null ? "~" + toType : "");
    }

    private int getNumALDsMatched(List<AdjListDescriptor> ALDs, Map<String, String> vertexMapping) {
        var fromVerticesInALDs = new HashSet<String>();
        ALDs.forEach(ALD -> fromVerticesInALDs.add(ALD.getFromQueryVertex()));
        var numALDsMatched = 0;
        for (var vertex : vertexMapping.keySet()) {
            if (fromVerticesInALDs.contains(vertex) && !vertexMapping.get(vertex).equals("")) {
                numALDsMatched++;
            }
        }
        return numALDsMatched;
    }

    /**
     * Fills the catalog stats: i-cost & selectivity.
     *
     * @param graph is the input data graph to get stats on.
     * @param store is the types and labels keyStore.
     * @param numThreads is the number of threads to use when executing the plan.
     */
    public void populate(Graph graph, KeyStore store, int numThreads, String filename)
        throws IOException, InterruptedException {
        var startTime = System.nanoTime();
        isAdjListSortedByType = graph.isAdjListSortedByType();
        sampledIcost = new HashMap<>();
        sampledSelectivity = new HashMap<>();
        inSubgraphs = new ArrayList<>();
        var plans = new CatalogPlans(graph, store, numSampledEdges, maxInputNumVertices);
        var queryPlan = new Plan[numThreads];
        var scans = plans.getScans();
        var queryGraphsToExtend = new QueryGraphSet();
        int scan_number=1;
        for (var scan : scans) {
            System.out.println("scan.getName: "+scan.getName());  //应该是 scan(a)->(b)
            System.out.println("This is the "+scan_number+" scan!!");
            var noop = new Noop(scan.getOutSubgraph());
            scan.setNext(noop);
            // noop 的作用可能是控制不同的type和label
            System.out.println("scan.getNext().length: "+scan.getNext().length);   // 期望结果是1, 代表一个scan后面的next只有一个noop
            noop.setPrev(scan);
            noop.setOutQVertexToIdxMap(scan.getOutQVertexToIdxMap());  // 如果没有 type 和 label, 则 OutQVertexToIdxMap=<{"a":0},{"b":1}>
            System.out.println("noop.getOutQVertexToIdxMap().size(): "+noop.getOutQVertexToIdxMap().size());  // 期望是2
            plans.setNextOperators(graph, noop, queryGraphsToExtend);  // scan-noop-intersectcatalog-noop-intersctcatalog-..., queryGraphsToExtend里存储查询子图

            System.out.println("\nsetnextoperators 结束\n");
            queryPlan[0] = new Plan(scan);  // scan 是 queryPlan 的 samplescan
            for (var i = 1; i < numThreads; i++) {
                queryPlan[i] = queryPlan[0].copyCatalogPlan();
            }
            setInputSubgraphs(queryGraphsToExtend.getQueryGraphSet());  // 把queryGraphsToExtend里所有querygraph加到this.inSubgraphs里
            System.out.println("inSubgraphs.size() = "+inSubgraphs.size());
            System.out.println("queryPlan.length = "+queryPlan.length+"\n");
            init(graph, store, queryPlan);  // probeTuble 的设立

            System.out.println("\n");
            execute(queryPlan);
            logOutput(graph, queryPlan);  // logOutput 调用 ddICostAndSelectivity
            scan_number++;
        }
        addZeroSelectivities(graph, plans);  // 一般的数据集, 这句话没有作用
        elapsedTime = IOUtils.getElapsedTimeInMillis(startTime);
        log(filename, numThreads, graph, store.getNextTypeKey(), store.getNextLabelKey());

        int fwd = 0;
        for(SortedAdjList adjlist : graph.getFwdAdjLists()){
            if(fwd < adjlist.size()){
                fwd = adjlist.size();

            }
        }
        System.out.println("fwd = "+fwd);

        int bwd = 0;
        for(SortedAdjList adjlist : graph.getBwdAdjLists()){
            if(bwd < adjlist.size()){
                bwd = adjlist.size();

            }
        }
        System.out.println("bwd = "+bwd);


    }

    private void init(Graph graph, KeyStore store, Plan[] queryPlanArr) {
        for (var queryPlan : queryPlanArr) {
            var probeTuple = new int[maxInputNumVertices + 1];  // 一般是 int[4]
             //  System.out.println("probeTuple.length = "+probeTuple.length);  结果是4
            queryPlan.getScanSampling().init(probeTuple, graph, store);  // getScanSampling() 得到的是 scan
        }
    }

    /**
     * Executes the {@link Plan}s.
     */
    public void execute(Plan[] queryPlanArr) throws InterruptedException {
        if (queryPlanArr.length > 1) {  // 多线程
            var threads = new Thread[queryPlanArr.length];
            for (var i = 0; i < threads.length; i++) {
                var sink = queryPlanArr[i].getSink();
                threads[i] = new Thread(() -> {
                    try { sink.execute(); } catch (LimitExceededException e) {/* nada. */}
                });
            }
            for (var thread : threads) {
                thread.start();
            }
            for (var thread : threads) {
                thread.join();
            }
        } else {  // 单线程
            System.out.println("单线程");
            var sink = queryPlanArr[0].getSink();
            try { sink.execute(); } catch (LimitExceededException e) {/* nada. */}
        }
    }

    private void logOutput(Graph graph, Plan[] queryPlanArr) {
        var operator = queryPlanArr[0].getSink().previous[0];
        while (!(operator instanceof ScanSampling)) {
            operator = operator.getPrev();
        }
        operator = operator.getNext(0); /* first noop */
        var other = new Operator[queryPlanArr.length - 1];
        for (var i = 1; i < queryPlanArr.length; i++) {
            other[i - 1] = queryPlanArr[i].getSink().previous[0];  // scan
            while (!(other[i - 1] instanceof ScanSampling)) {
                other[i - 1] = other[i - 1].getPrev();
            }
            other[i - 1] = other[i - 1].getNext(0); /* first noop */
        }
        if (isAdjListSortedByType) {
            addICostAndSelectivitySortedByType(operator, other, graph.isUndirected());
        } else {
            System.out.println("logoutput 时走的时 isadjlistsortedbytype = false");
            addICostAndSelectivity(operator, other, graph.isUndirected());  // other 里是 noop
        }
    }

    private void addZeroSelectivities(Graph graph, CatalogPlans plans) {
        var selectivityZero = plans.getSelectivityZero();
        for (var select : selectivityZero) {
            var subgraphIdx = getSubgraphIdx(select.a);
            sampledSelectivity.putIfAbsent(subgraphIdx, new HashMap<>());
            var ALDsAsStrList = new ArrayList<String>();
            var ALDsStr = getALDsAsStr(select.b /*ALDs*/, null, null);
            if (graph.isUndirected()) {
                var splits = ALDsStr.split(", ");
                var directionPatterns = CatalogPlans.generateDirectionPatterns(splits.length,
                    graph.isUndirected());
                for (var pattern : directionPatterns) {
                    var ALDsStrWithPattern = "";
                    for (var j = 0; j < pattern.length; j++) {
                        var ok = splits[j].split("Bwd");
                        if (j == pattern.length - 1) {
                            ALDsStrWithPattern += ok[0] + pattern[j] + ok[1];
                        } else {
                            ALDsStrWithPattern += ok[0] + pattern[j] + ok[1] + ", ";
                        }
                    }
                    ALDsAsStrList.add(ALDsStrWithPattern);
                }
            } else {
                ALDsAsStrList.add(ALDsStr);
            }
            for (var ALDsAsStr : ALDsAsStrList) {
                sampledSelectivity.get(subgraphIdx).put(ALDsAsStr + "~" + select.c,
                    0.00 /* selectivity */);
            }
        }
    }

    private void addICostAndSelectivity(Operator operator, Operator[] other, boolean isUndirected) {
        if (operator.getNext()[0] instanceof Sink) {
            return;   // other 应该是 Noop
        }
        var numInputTuples = operator.getNumOutTuples();
        for (var otherOperator : other) {
            numInputTuples += otherOperator.getNumOutTuples();
        }
        var inSubgraph = operator.getOutSubgraph();
        var subgraphIdx = getSubgraphIdx(inSubgraph);
        var next = operator.getNext();
        for (var i = 0; i < next.length; i++) {
            var intersect = (IntersectCatalog) next[i];
            var ALDs = intersect.getALDs();
            var ALDsAsStrList = new ArrayList<String>();
            var ALDsStr = getALDsAsStr(intersect.getALDs(), null, null);
            if (isUndirected) {
                var splits = ALDsStr.split(", ");
                var directionPatterns = CatalogPlans.generateDirectionPatterns(splits.length,
                    isUndirected);
                for (var pattern : directionPatterns) {
                    var ALDsStrWithPattern = "";
                    for (var j = 0; j < pattern.length; j++) {
                        var ok = splits[j].split("Bwd");
                        if (j == pattern.length - 1) {
                            ALDsStrWithPattern += ok[0] + pattern[j] + ok[1];
                        } else {
                            ALDsStrWithPattern += ok[0] + pattern[j] + ok[1] + ", ";
                        }
                    }
                    ALDsAsStrList.add(ALDsStrWithPattern);
                }
            } else {
                ALDsAsStrList.add(ALDsStr);
            }
            if (1 == ALDs.size()) {

                // 只有一个 ALD 时, 才放入 sampledIcost
                var icost = next[i].getIcost();
                for (var otherOperator : other) {
                    icost += otherOperator.getNext(i).getIcost();
                }
                sampledIcost.putIfAbsent(subgraphIdx, new HashMap<>());
                if (numInputTuples > 0) {
                    for (var ALDsAsStr : ALDsAsStrList) {
                        sampledIcost.get(subgraphIdx).putIfAbsent(ALDsAsStr, /*avg estimatedIcost*/
                            icost / (double) numInputTuples);
                    }
                } else {
                    for (var ALDsAsStr : ALDsAsStrList) {
                        sampledIcost.get(subgraphIdx).putIfAbsent(ALDsAsStr, 0.0);
                    }
                }
            }
            var noops = next[i].getNext();
            for (short toType = 0; toType < noops.length; toType++) {
                var noop = noops[toType];
                var selectivity = noop.getNumOutTuples();
                for (var otherOperator : other) {
                    selectivity += otherOperator.getNext(i).getNext(toType).getNumOutTuples();
                }
                sampledSelectivity.putIfAbsent(subgraphIdx, new HashMap<>());
                if (numInputTuples > 0) {
                    for (var ALDsAsStr : ALDsAsStrList) {
                        sampledSelectivity.get(subgraphIdx).put(ALDsAsStr + "~" + toType,
                            selectivity / (double) numInputTuples /* avg estimated selectivity */);
                    }
                } else {
                    for (var ALDsAsStr : ALDsAsStrList) {
                        sampledSelectivity.get(subgraphIdx).put(ALDsAsStr + "~" + toType, 0.0);
                    }
                }
                var otherNoops = new Noop[other.length];
                for (var j = 0; j < otherNoops.length; j++) {
                    otherNoops[j] = (Noop) other[j].getNext(i).getNext(toType);
                }
                addICostAndSelectivity(noop, otherNoops, isUndirected);
            }
        }
    }

    private void addICostAndSelectivitySortedByType(Operator operator, Operator[] other,
        boolean isUndirected) {
        if (operator.getNext()[0] instanceof Sink) {
            return;
        }
        var numInputTuples = operator.getNumOutTuples();
        for (var otherOperator : other) {
            numInputTuples += otherOperator.getNumOutTuples();
        }
        var inSubgraph = operator.getOutSubgraph();
        var subgraphIdx = getSubgraphIdx(inSubgraph);
        var next = operator.getNext();
        for (var i = 0; i < next.length; i++) {
            var intersect = (IntersectCatalog) next[i];
            var ALDs = intersect.getALDs();
            var toType = intersect.getToType();
            var ALDsAsStrList = new ArrayList<String>();
            var ALDsStr = getALDsAsStr(ALDs, null, toType);
            if (isUndirected) {
                var splits = ALDsStr.split(", ");
                var directionPatterns = CatalogPlans.generateDirectionPatterns(splits.length,
                    isUndirected);
                for (var pattern : directionPatterns) {
                    var ALDsStrWithPattern = "";
                    for (var j = 0; j < pattern.length; j++) {
                        var ok = splits[j].split("Bwd");
                        if (j == pattern.length - 1) {
                            ALDsStrWithPattern += ok[0] + pattern[j] + ok[1];
                        } else {
                            ALDsStrWithPattern += ok[0] + pattern[j] + ok[1] + ", ";
                        }
                    }
                    ALDsAsStrList.add(ALDsStrWithPattern);
                }
            } else {
                ALDsAsStrList.add(ALDsStr);
            }
            if (1 == ALDs.size()) {
                var icost = next[i].getIcost();
                for (var otherOperator : other) {
                    icost += otherOperator.getNext(i).getIcost();
                }
                sampledIcost.putIfAbsent(subgraphIdx, new HashMap<>());
                if (numInputTuples > 0) {
                    for (var ALDsAsStr : ALDsAsStrList) {
                        sampledIcost.get(subgraphIdx).putIfAbsent(ALDsAsStr, /*avg estimatedIcost*/
                            icost / (double) numInputTuples);
                    }
                } else {
                    for (var ALDsAsStr : ALDsAsStrList) {
                        sampledIcost.get(subgraphIdx).putIfAbsent(ALDsAsStr, 0.0);
                    }
                }
            }
            var selectivity = intersect.getNumOutTuples();
            for (var otherOperator : other) {
                selectivity += otherOperator.getNext(i).getNumOutTuples();
            }
            sampledSelectivity.putIfAbsent(subgraphIdx, new HashMap<>());
            if (numInputTuples > 0) {
                for (var ALDsAsStr : ALDsAsStrList) {
                    sampledSelectivity.get(subgraphIdx).put(ALDsAsStr,
                        selectivity / (double) numInputTuples /* avg estimated selectivity */);
                }
            } else {
                for (var ALDsAsStr : ALDsAsStrList) {
                    sampledSelectivity.get(subgraphIdx).put(ALDsAsStr, 0.0);
                }
            }
            var noop = next[i].getNext()[0];
            var otherNoops = new Noop[other.length];
            for (var j = 0; j < otherNoops.length; j++) {
                otherNoops[j] = (Noop) other[j].getNext(i).getNext()[0];
            }
            addICostAndSelectivitySortedByType(noop, otherNoops, isUndirected);
        }
    }

    private int getSubgraphIdx(QueryGraph inSubgraph) {
        for (var i = 0; i < inSubgraphs.size(); i++) {
            if (inSubgraph.isIsomorphicTo(inSubgraphs.get(i))) {
                return i;
            }
        }
        throw new IllegalArgumentException();
    }

    private void setInputSubgraphs(Set<QueryGraph> inSubgraphs) {
        for (var inSubgraph : inSubgraphs) {
            var isUnique = true;
            for (var thisInSubgraph : this.inSubgraphs) {
                if (thisInSubgraph.isIsomorphicTo(inSubgraph)) {
                    isUnique = false;
                    break;
                }
            }
            if (isUnique) {
                this.inSubgraphs.add(inSubgraph);
            }
        }
    }

    private void log(String filename, int numThreads, Graph graph, int numTypes, int numLabels)
        throws IOException {
        IOUtils.createNewFile(filename);
        var writer = new BufferedWriter(new FileWriter(filename));
        var header = "i-cost & selectivity of ALDs";
        if (numSampledEdges < graph.getNumEdges()) {
            header += " (scanned a sample of " + numSampledEdges + " edges from " +
                graph.getNumEdges() + ").";
        } else {
            header += "(over the whole graph).";
        }
        writer.write("The catalog was generated in " + String.format("%.2f", elapsedTime / 1000) +
            " secs & number of threads used was " + numThreads + ".\n\n" + header + "\n" +
            String.join("", Collections.nCopies(header.length() + 1, "~")) + "\n\n");
        var i = 0;
        for (var inSubgraph : inSubgraphs) {
            var inSubgraphAsStr = inSubgraph.toStringWithTypesAndLabels();
            writer.write(inSubgraphAsStr + ":\n" +
                String.join("", Collections.nCopies(inSubgraphAsStr.length() + 1, "-")) + "\n\n");

            writer.write("* I-Cost:\n");
            if (isAdjListSortedByType) {
                for (var ALD : sampledSelectivity.get(i).keySet()) {
                    if (ALD.chars().filter(ch -> ch == '(').count() > 1) {
                        continue;
                    }
                    var splits = ALD.split("~");
                    var icost = String.format("%.2f", sampledSelectivity.get(i).get(ALD));
                    String ALDasStr = splits[0] + (numTypes > 1 ? " (" + splits[1] + ")" : "");
                    writer.write(ALDasStr + " : " + icost + "\n");
                }
            } else {
                for (var ALD : sampledIcost.get(i).keySet()) {
                    var icost = String.format("%.2f", sampledIcost.get(i).get(ALD));
                    writer.write(ALD + " : " + icost + "\n");
                }
            }
            writer.write("\n");

            writer.write("* Selectivity:\n");
            for (var ALDsAndToType : sampledSelectivity.get(i).keySet()) {
                var splits = ALDsAndToType.split("~");
                var icost = String.format("%.2f", sampledSelectivity.get(i).get(ALDsAndToType));
                var ALDasStr = splits[0] + (numTypes > 1 ? " (" + splits[1] + ")" : "");
                writer.write(ALDasStr + " : " + icost + "\n");
            }
            writer.write("\n");
            i++;
        }
        writer.flush();
        writer.close();
    }

    public void serialize(String directoryPath) throws IOException {
        logger.info("serializing the data graph's catalog.");
        IOUtils.serializeObjs(directoryPath, new Object[] {
            /* <filename , field to serialize> pair */
            "icost_" + numSampledEdges, sampledIcost,
            "selectivity_" + numSampledEdges, sampledSelectivity,
            "inSubgraphs", inSubgraphs,
            "isAdjListSortedByType", isAdjListSortedByType,
            "numSampledEdges", numSampledEdges,
            "maxInputNumVertices", maxInputNumVertices
        });
    }
}
