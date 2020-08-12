package ca.waterloo.dsg.graphflow.plan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator.LimitExceededException;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.Build;
import ca.waterloo.dsg.graphflow.plan.operator.hashjoin.HashTable;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanBlocking;
import ca.waterloo.dsg.graphflow.plan.operator.scan.ScanBlocking.VertexIdxLimits;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import ca.waterloo.dsg.graphflow.util.container.Triple;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Query plan workers execute a query plan in parallel given a number of threads.
 */
public class Workers {

    protected static final Logger logger = LogManager.getLogger(Workers.class);

    private Plan[] queryPlans;
    private Thread[][] workers;
    private int numThreads = 1;

    @Getter private double elapsedTime = 0;
    private long intersectionCost = 0;
    private long numIntermediateTuples = 0;
    private long numOutTuples = 0;
    transient private List<Triple<String /* name */,
        Long /* i-cost */, Long /* prefixes size */>> operatorMetrics;

    /**
     * Constructs a {@link Workers} object.
     *
     * @param queryPlan is the query plan to execute.
     * @param numThreads is the number of threads to use executing the query.
     */
    public Workers(Plan queryPlan, int numThreads) {
        queryPlans = new Plan[numThreads];
        if (numThreads == 1) {
           queryPlans[0] = queryPlan;
        } else { // numThreads > 1
            for (int i = 0; i < numThreads; i++) {
                queryPlans[i] = queryPlan.copy(true /* isThreadSafe */);
            }
            this.numThreads = numThreads;
            var numSubplans = queryPlans[0].getSubplans().size();
            workers = new Thread[numSubplans][numThreads];
            for (var i = 0; i < queryPlans.length; i++) {
                var subplans = queryPlans[i].getSubplans();
                for (var subplanId = 0; subplanId < numSubplans; subplanId++) {
                    var operator = subplans.get(subplanId);
                    Runnable runnable = () -> {
                        try { operator.execute(); } catch (LimitExceededException e) {}
                    };
                    workers[subplanId][i] = new Thread(runnable);
                }
            }
            for (var i = 0; i < numSubplans; i++) {
                var globalVertexIdxLimits = new VertexIdxLimits();
                for (var plan : queryPlans) {
                    var operator = plan.subplans.get(i);
                    while (null != operator.getPrev()) {
                        operator = operator.getPrev();
                    }
                    if (operator instanceof ScanBlocking) {
                        ((ScanBlocking) operator).setGlobalVerticesIdxLimits(globalVertexIdxLimits);
                    }
                }
            }
        }
    }

    public void init(Graph graph, KeyStore store) {
        for (var queryPlan : queryPlans) {
            queryPlan.init(graph, store);
        }
        var numBuildOperators = queryPlans[0].getSubplans().size() - 1;
        for (var buildIdx = 0; buildIdx < numBuildOperators; buildIdx++) {
            var ID = ((Build) queryPlans[0].getSubplans().get(buildIdx)).getID();
            var hashTables = new HashTable[numThreads];
            for (var i = 0; i < queryPlans.length; i++) {
                hashTables[i] = ((Build) queryPlans[i].getSubplans().get(buildIdx)).getHashTable();
            }
            for (var queryPlan : queryPlans) {
                queryPlan.setProbeHashTables(ID, hashTables);
            }
        }
    }

    public void execute() throws InterruptedException {
        if (queryPlans.length == 1) {
            queryPlans[0].execute();
            elapsedTime = queryPlans[0].getElapsedTime();
        } else {
            var beginTime = System.nanoTime();
            for (var subplanWorkers : workers) {
                for (int j = 0; j < queryPlans.length; j++) {
                    subplanWorkers[j].start();
                }
                for (int j = 0; j < queryPlans.length; j++) {
                    subplanWorkers[j].join();
                }
            }
            elapsedTime = IOUtils.getElapsedTimeInMillis(beginTime);
        }
    }

    /**
     * @return The stats as a one line comma separated CSV  one line row for logging.
     */
    public String getOutputLog() {
        if (queryPlans.length == 1) {
            return queryPlans[0].getOutputLog();
        }
        if (null == operatorMetrics) {
            operatorMetrics = new ArrayList<>();
            for (var queryPlan : queryPlans) {
                queryPlan.setStats();
            }
            aggregateOutput();
        }
        var strJoiner = new StringJoiner(",");
        strJoiner.add(String.format("%.4f", elapsedTime));
        strJoiner.add(String.format("%d", numOutTuples));
        strJoiner.add(String.format("%d", numIntermediateTuples));
        strJoiner.add(String.format("%d", intersectionCost));
        for (var operatorMetric : operatorMetrics) {
            strJoiner.add(String.format("%s", operatorMetric.a));     /* operator name */
        }
        return strJoiner.toString() + "\n";
    }

    private void aggregateOutput() {
        operatorMetrics = new ArrayList<>();
        for (var queryPlan : queryPlans) {
            intersectionCost += queryPlan.getIcost();
            numIntermediateTuples += queryPlan.getNumIntermediateTuples();
            numOutTuples += queryPlan.getLastOperator().getNumOutTuples();
        }
        var queryPlan = queryPlans[0];
        for (var metric : queryPlan.getOperatorMetrics()) {
            operatorMetrics.add(new Triple<>(metric.a, metric.b, metric.c));
        }
        for (int i = 1; i < queryPlans.length; i++) {
            for (int j = 0; j < operatorMetrics.size(); j++) {
                operatorMetrics.get(j).b += queryPlans[i].getOperatorMetrics().get(j).b;
                operatorMetrics.get(j).c += queryPlans[i].getOperatorMetrics().get(j).c;
            }
        }
    }
}
