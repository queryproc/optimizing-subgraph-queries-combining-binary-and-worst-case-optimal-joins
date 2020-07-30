package ca.waterloo.dsg.graphflow.planner.catalog.operator;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.var;

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
}
