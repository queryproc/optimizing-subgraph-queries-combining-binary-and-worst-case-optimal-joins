package ca.waterloo.dsg.graphflow.plan.operator;

import ca.waterloo.dsg.graphflow.plan.operator.sink.Sink;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.util.container.Triple;
import lombok.Getter;
import lombok.Setter;
import lombok.var;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for all database operators.
 */
public abstract class Operator implements Serializable {

    protected final boolean IS_PROFILED = false;

    /**
     * Limit exception thrown for LIMIT queries.
     */
    public class LimitExceededException extends Exception {}

    public static boolean CACHING_ENABLED = true;

    @Getter protected String name;
    @Getter protected Operator[] next;
    @Getter @Setter protected Operator prev;

    @Setter protected int[] probeTuple;

    @Getter protected int outTupleLen;
    @Getter protected QueryGraph inSubgraph;
    @Getter @Setter protected QueryGraph outSubgraph;
    @Getter @Setter protected Map<String, Integer> outQVertexToIdxMap;
    @Getter @Setter protected int lastRepeatedVertexIdx;

    @Getter protected long numOutTuples = 0;
    @Getter protected long icost = 0;

    /**
     * Constructs an {@link Operator} object.
     *
     * @param outSubgraph The subgraph matched by the output tuples.
     * @param inSubgraph The subgraph matched by the input tuples.
     */
    protected Operator(QueryGraph outSubgraph, QueryGraph inSubgraph) {
        this.outSubgraph = outSubgraph;
        this.inSubgraph = inSubgraph;
        this.outTupleLen = outSubgraph.getNumVertices();
    }

    public Set<String> getOutQVertices() {
        return outQVertexToIdxMap.keySet();
    }

    /**
     * Constructs an {@link Operator} object.
     */
    protected Operator() {}

    /**
     * Initialize the operator e.g. memory allocation.
     *
     * @param probeTuple is the tuple processed throughout the query plan.
     * @param graph is the input data graph.
     * @param store is the labels and types key store.
     */
    public abstract void init(int[] probeTuple, Graph graph, KeyStore store);

    /**
     * Checks if the two plans, the one with this operator as root and the one with root
     * as passed operator are the same plans. The function relies in its checks on a set of
     * invariants across the code base for each operator.
     *
     * @param operator The other operator to compare against.
     * @return True, if the plans with these operators as root are the same. False, otherwise.
     */
    public boolean isSameAs(Operator operator) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param index The index of the next operator to return.
     * @return The {@link Operator} at the given index.
     */
    public Operator getNext(int index) {
        return next[index];
    }

    /**
     * @param operator The next operator to append prefixes to.
     */
    public void setNext(Operator operator) {
        next = new Operator[] { operator };
    }

    /**
     * @param operators The next operator to append prefixes to.
     */
    public void setNext(Operator[] operators) {
        next = operators;
    }

    /**
     * Process a new tuple and push the produced tuples to the next operator.
     */
    public abstract void processNewTuple() throws LimitExceededException;

    /**
     * Executes the operator.
     */
     public void execute() throws LimitExceededException {
        if (null != prev) {
            prev.execute();
        }
    }

    public String getALDsAsString() {
        return "";
    }

    public void updateOperatorName(Map<String, Integer> queryVertexToIndexMap) {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() +
            " does not support updateOperatorName(Map<String, Integer> queryVertexToIndexMap).");
    }

    /**
     * Fills the operator metrics and recursively calls its prev operators to do the same.
     *
     * @param operatorMetrics The List of triple {@code String} operator name, {@code Long}
     * intersection cost, and {@code Long} probeTuple output size.
     */
    public void getOperatorMetricsNextOperators(List<Triple<String, Long, Long>> operatorMetrics) {
        operatorMetrics.add(new Triple<>(name, icost, numOutTuples));
        if (null != next) {
            for (Operator nextOperator : next) {
                if (!(nextOperator instanceof Sink)) {
                    nextOperator.getOperatorMetricsNextOperators(operatorMetrics);
                }
            }
        }
    }

    public void getLastOperators(List<Operator> lastOperators) {
        if (next != null) {
            for (var nextOperator : next) {
                nextOperator.getLastOperators(lastOperators);
            }
        } else {
            lastOperators.add(this);
        }
    }

    /**
     * @return The number of intersect operators before and including this operator in the query
     * transform.
     */
    public boolean hasMultiEdgeExtends() {
        if (null != prev) {
            return prev.hasMultiEdgeExtends();
        }
        return false;
    }

    /**
     * Creates a copy of the operator and same recursively of the prev operators referenced
     * for single or multi-threaded execution.
     *
     * @param isThreadSafe specifies whether to copy each operator as blocking operator or not.
     * @return The copy of the operator.
     */
    public Operator copy(boolean isThreadSafe) {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a copy of the operator and same recursively of the prev operators referenced
     * for single-threaded execution.
     */
    public Operator copy() {
        return copy(false);
    }
}
