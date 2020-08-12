package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.Operator.LimitExceededException;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.Build;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.HashTable;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.Probe;
import ca.waterloo.dsg.graphflow.plan.operator.scan.Scan;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanSampling;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink;
import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink.SinkType;
import ca.waterloo.dsg.graphflow.plan.operator.sink.SinkLimit;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Triple;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Query Plan (QP) representing left-deep binary plans, bushy binary plans, worst-case optimal
 * plans, and hybrid plans.
 */
public class Plan implements Serializable {

    @Getter private Sink sink;
    @Setter public SinkType sinkType = SinkType.COUNTER;
    @Getter public ScanSampling scanSampling;
    @Getter private Operator lastOperator;
    @Setter public int outTuplesLimit;

    @Getter private double elapsedTime = 0;
    @Getter private long icost = 0;
    @Getter private long numIntermediateTuples = 0;
    @Getter private long numOutTuples = 0;
    @Getter transient private List<Triple<String /* name */,
        Long /* i-cost */, Long /* number output tuples */>> operatorMetrics = new ArrayList<>();

    private boolean executed = false;
    @Getter private boolean adaptiveEnabled = false;

    @Getter List<Operator> subplans = new ArrayList<>();
    private List<Probe> probes;

    @Getter @Setter double estimatedICost;
    @Getter @Setter double estimatedNumOutTuples;
    @Getter @Setter Map<String/*qVertex*/, Double/*estimatedNumOutTuples*/> qVertexToNumOutTuples;

    /**
     * Constructs a {@link Plan} object.
     */
    public Plan(ScanSampling scan) {
        this.scanSampling = scan;
        var lastOperators = new ArrayList<Operator>();
        scan.getLastOperators(lastOperators);
        var outSubgraph = lastOperators.get(0).getOutSubgraph();
        sink = new Sink(outSubgraph);
        sink.previous = lastOperators.toArray(new Operator[0]);
        for (var lastOperator : lastOperators) {
            lastOperator.setNext(sink);
        }
    }

    /**
     * Constructs a {@link Plan} object.
     *
     * @param lastOperator is the operator to execute.
     */
    public Plan(Operator lastOperator) {
        this.subplans.add(lastOperator);
        this.lastOperator = lastOperator;
    }

    /**
     * Constructs a {@link Plan} object.
     *
     * @param subplans are the setAdjListSortOrder of linear subplans making up the query plan.
     */
    public Plan(List<Operator> subplans) {
        this.subplans = subplans;
        this.lastOperator = subplans.get(subplans.size() - 1);
    }

    /**
     * Constructs a {@link Plan} object.
     *
     * @param lastOperator is the scan operator to execute.
     * @param estimatedNumOutTuples is the number of output tuples from the scan.
     */
    public Plan(Scan lastOperator, double estimatedNumOutTuples) {
        this(lastOperator);
        this.estimatedNumOutTuples = estimatedNumOutTuples;
        qVertexToNumOutTuples = new HashMap<>();
        qVertexToNumOutTuples.put(lastOperator.getFromQueryVertex(), estimatedNumOutTuples);
        qVertexToNumOutTuples.put(lastOperator.getToQueryVertex(), estimatedNumOutTuples);
    }

    public void append(Operator newOperator) {
        lastOperator.setNext(newOperator);
        newOperator.setPrev(lastOperator);
        subplans.set(subplans.size() - 1, newOperator);
        lastOperator = newOperator;
    }

    /**
     * Executes the {@link Plan}.
     */
    public void execute() {
        if (SinkType.LIMIT != sinkType) {
            var startTime = System.nanoTime();
            try {
                for (var subplan : subplans) {
                    subplan.execute();
                }
            } catch (LimitExceededException e) {} // never thrown.
            elapsedTime = IOUtils.getElapsedTimeInMillis(startTime);
        } else {
            ((SinkLimit) sink).setStartTime(System.nanoTime());
            try {
                for (var subplan : subplans) {
                    subplan.execute();
                }
            } catch (LimitExceededException e) {} // never thrown.
            elapsedTime = ((SinkLimit) sink).getElapsedTime();
        }
        executed = true;
        numOutTuples = sink.getNumOutTuples();
    }

    /**
     * Initialize the plan by initializing all of its operators.
     *
     * @param graph is the input data graph.
     * @param store is the labels and types key store.
     */
    public void init(Graph graph, KeyStore store) {
        var lastOperator = subplans.get(subplans.size() - 1);
        var queryGraph = lastOperator.getOutSubgraph();
        switch(sinkType) {
            case LIMIT:
                sink = new SinkLimit(queryGraph, outTuplesLimit);
                break;
            case COUNTER:
            default:
                sink = new Sink(queryGraph);
                break;
        }
        sink.setPrev(lastOperator);
        lastOperator.setNext(sink);
        probes = new ArrayList<>();
        for (int i = 1; i < subplans.size(); i++) {
            var operator = subplans.get(i);
            if (operator instanceof Probe) {
                probes.add((Probe) operator);
            }
            while (null != operator.getPrev()) {
                operator = operator.getPrev();
                if (operator instanceof Probe) {
                    probes.add((Probe) operator);
                }
            }
        }
        for (int i = 0; i < subplans.size() - 1; i++) {
            var build = (Build) subplans.get(i);
            var hashTable = new HashTable(build.getBuildHashIdx(), build.getHashedTupleLen());
            build.setHashTable(hashTable);
        }
        for (var subplan : subplans) {
            var probeTuple = new int[subplan.getOutTupleLen()];
            var firstOperator = subplan;
            while (null != firstOperator.getPrev()) {
                firstOperator = firstOperator.getPrev();
            }
            firstOperator.init(probeTuple, graph, store);
        }
    }

    void setProbeHashTables(int ID, HashTable[] hashTables) {
        for (var probe : probes) {
            if (probe.getID() == ID) {
                probe.setHashTables(hashTables);
            }
        }
    }

    /**
     * @return The stats as a one line comma separated CSV  one line row for logging.
     */
    public String getOutputLog() {
        if (null == operatorMetrics) {
            operatorMetrics = new ArrayList<>();
        }
        setStats();
        var strJoiner = new StringJoiner(",");
        if (executed) {
            strJoiner.add(String.format("%.4f", elapsedTime));
            strJoiner.add(String.format("%d", numOutTuples));
            strJoiner.add(String.format("%d", numIntermediateTuples));
            strJoiner.add(String.format("%d", icost));
        }
        for (var operatorMetric : operatorMetrics) {
            strJoiner.add(String.format("%s", operatorMetric.a));     /* operator name */
        }
        return strJoiner.toString() + "\n";
    }

    void setStats() {
        for (var subplan : subplans) {
            var firstOperator = subplan;
            while (null != firstOperator.getPrev()) {
                firstOperator = firstOperator.getPrev();
            }
            firstOperator.getOperatorMetricsNextOperators(operatorMetrics);
        }
        for (var i = 0; i < operatorMetrics.size() - 1; ++i) {
            icost += operatorMetrics.get(i).b;
            numIntermediateTuples += operatorMetrics.get(i).c;
        }
        icost += operatorMetrics.get(operatorMetrics.size() - 1).b;
    }

    /**
     * Checks if the two plans are equivalent.
     *
     * @param otherQueryPlan is the other query plan to compare against.
     * @return True, if the two plans are equivalent. False, otherwise.
     */
    public boolean isSameAs(Plan otherQueryPlan) {
        if (subplans.size() != otherQueryPlan.subplans.size()) {
            return false;
        }
        for (var i = 0; i < subplans.size(); i++) {
            if (!subplans.get(i).isSameAs(otherQueryPlan.subplans.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Shallow copy of the query plan.
     */
    public Plan shallowCopy() {
        return new Plan(new ArrayList<>(this.subplans));
    }

    /**
     * Deep copy of the query plan.
     *
     * @param isThreadSafe specifies whether scans and hash joins are thread safe or not.
     */
    public Plan copy(boolean isThreadSafe) {
        var subplans = new ArrayList<Operator>(this.subplans.size());
        for (var subplan : this.subplans) {
            subplans.add(subplan.copy(isThreadSafe));
        }
        return new Plan(subplans);
    }

    /**
     * Deep copy of the query plan.
     */
    public Plan copy() {
        return copy(false);
    }

    public Plan copyCatalogPlan() {
        return new Plan(scanSampling.copy());
    }
}
