package ca.waterloo.dsg.graphflow.plan.operator.scan;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.var;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Samples edges from an array of edges. Edges are pushed to the next operators one at a time.
 */
public class ScanSampling extends Scan {

    private BlockingQueue<int[]> edgesQueue;

    /**
     * Constructs a {@link ScanSampling} operator.
     *
     * @param outSubgraph is the subgraph, with one query edge, matched by the output tuples.
     */
    public ScanSampling(QueryGraph outSubgraph) {
        super(outSubgraph);
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        if (null == this.probeTuple) {
            this.probeTuple = probeTuple;
            for (var nextOperator : next) {
                nextOperator.init(probeTuple, graph, store);
            }
        }
    }

    /**
     * @see Operator#execute().
     */
    @Override
    public void execute() throws LimitExceededException {
        try {
            while (true) {
                var edge = edgesQueue.remove(); // NoSuchElementException if empty.
                probeTuple[0] = edge[0];
                probeTuple[1] = edge[1];
                numOutTuples++;
                for (var nextOperator : next) {
                    nextOperator.processNewTuple();
                }
            }
        } catch (NoSuchElementException e) {
            // queue is empty.
        }
    }

    /**
     * @param edges is a list of edges to sample from.
     * @param numEdgesToSample is the number of edges to sample.
     */
    public void setEdgeIndicesToSample(int[] edges, int numEdgesToSample) {
        var randomNumGen = new Random(0 /*Always same seed for reproducibility*/);
        var numEdges = edges.length / 2;
        edgesQueue = new LinkedBlockingQueue<>(numEdgesToSample);
        while (edgesQueue.size() < numEdgesToSample) {
            var edgeIdx = randomNumGen.nextInt(numEdges);
            edgesQueue.add(new int[] {
                edges[edgeIdx * 2]     /* fromVertex */,
                edges[edgeIdx * 2 + 1] /* toVertex   */
            });
        }
    }

    /**
     * @param edges is a list of edges to sample from.
     * @param numEdgesToSample is the number of edges to sample.
     */
    public void setEdgeIndicesToSample(List<int[]> edges, int numEdgesToSample) {
        var randomNumGen = new Random(0 /*Always same seed for reproducibility*/);
        edgesQueue = new LinkedBlockingQueue<>(numEdgesToSample);
        while (edgesQueue.size() < numEdgesToSample) {
            var edgeIdx = randomNumGen.nextInt(edges.size());
            edgesQueue.add(edges.get(edgeIdx));
        }
    }

    /**
     * @see Operator#copy()
     */
    @Override
    public ScanSampling copy() {
        var scanSampling = new ScanSampling(outSubgraph);
        scanSampling.edgesQueue = edgesQueue;
        return scanSampling;
    }
}
