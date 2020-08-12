package ca.waterloo.dsg.graphflow.planner.catalog.operator;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;

public class Noop extends Operator {

    /**
     * @param queryGraph is the input and output {@link QueryGraph}.
     */
    public Noop(QueryGraph queryGraph) {
        super(queryGraph, queryGraph);
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        this.probeTuple = probeTuple;
        for (var nextOperator : next) {
            nextOperator.init(probeTuple, graph, store);
        }
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() throws LimitExceededException {
        numOutTuples++;
        for (var nextOperator : next) {
            nextOperator.processNewTuple();
        }
    }

    /**
     * @see Operator#copy()
     */
    @Override
    public Noop copy() {
        var copy = new Noop(outSubgraph);
        if (null != next) {
            var nextCopy = new Operator[next.length];
            for (var i = 0; i < next.length; i++) {
                nextCopy[i] = next[i].copy();
            }
            copy.setNext(nextCopy);
            for (var nextOp : nextCopy) {
                nextOp.setPrev(copy);
            }
        }
        return copy;
    }
}
