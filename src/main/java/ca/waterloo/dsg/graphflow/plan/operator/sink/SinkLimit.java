package ca.waterloo.dsg.graphflow.plan.operator.sink;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.Getter;
import lombok.Setter;

/**
 * A sink operator stopping the query plan execution once a number of output tuples is reached.
 */
public class SinkLimit extends Sink {

    @Setter public long startTime;
    @Getter public double elapsedTime;
    @Setter public int outTuplesLimit;

    /**
     * Constructs a {@link SinkLimit} object.
     *
     * @param queryGraph is the {@link QueryGraph}, the tuples in the sink match.
     * @param outTuplesLimit is the number of output tuples the query is limited to.
     */
    public SinkLimit(QueryGraph queryGraph, int outTuplesLimit) {
        super(queryGraph);
        this.outTuplesLimit = outTuplesLimit;
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() throws LimitExceededException {
        if (prev.getNumOutTuples() >= outTuplesLimit) {
            elapsedTime = IOUtils.getElapsedTimeInMillis(startTime);
            throw new LimitExceededException();
        }
    }
}
