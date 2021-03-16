package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.planner.catalog.operator.Noop;
import ca.waterloo.dsg.graphflow.query.QueryEdge;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;

/**
 * An base sink collecting the output results from the dataflow acting as a count(*).
 */
public class Sink extends Operator {

    /**
     * The different types of sink operators.
     */
    public enum SinkType {
        LIMIT,
        COUNTER /* default */
    }

    public Operator[] previous;

    /**
     * Constructs a {@link Sink} object.
     *
     * @param queryGraph is the {@link QueryGraph}, the tuples in the sink match.
     */
    public Sink(QueryGraph queryGraph) {
        super(queryGraph, queryGraph);
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        if (null == this.probeTuple) {
            this.probeTuple = probeTuple;
        }
    }

    /**
     * Executes the operator.
     */
    public void execute() throws LimitExceededException {
        if (null != previous) {
            System.out.println("走的是 previous");
            System.out.println("是不是noop: "+ (previous[0] instanceof Noop));

            System.out.println("Edges:");
            for(QueryEdge queryedge : previous[0].getOutSubgraph().getEdges()){
                System.out.println(queryedge.getFromVertex() + queryedge.getToVertex());
            }
            System.out.println("\n");
            previous[0].execute();
        } else {
            System.out.println("是不是noop:"+ (prev instanceof Noop));
            prev.execute();
        }
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() throws LimitExceededException {}

    @Override
    public long getNumOutTuples() {
        if (null != previous) {
            var numOutTuples = 0;
            for (var previousOperator : previous) {
                numOutTuples += previousOperator.getNumOutTuples();
            }
            return numOutTuples;
        }
        return prev.getNumOutTuples();
    }

    /**
     * @see Sink#isSameAs(Operator)
     */
    @Override
    public boolean isSameAs(Operator operator) {
        return operator instanceof Sink &&
            this.getPrev().isSameAs(operator.getPrev());
    }

    /**
     * @see Operator#copy(boolean)
     */
    @Override
    public Sink copy(boolean isThreadSafe) {
        var sink = new Sink(outSubgraph);
        sink.prev = this.prev.copy(isThreadSafe);
        return sink;
    }

    /**
     * @see Operator#copy(boolean)
     */
    @Override
    public Sink copy() {
        return new Sink(outSubgraph);
    }
}
