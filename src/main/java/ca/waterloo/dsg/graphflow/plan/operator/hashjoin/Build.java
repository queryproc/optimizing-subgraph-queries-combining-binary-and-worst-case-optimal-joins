package ca.waterloo.dsg.graphflow.plan.operator.hashjoin;

import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.Getter;
import lombok.Setter;
import lombok.var;

import java.io.Serializable;

/**
 * An operator building a hash table by hashing incoming tuples on a single attribute.
 */
public class Build extends Operator implements Serializable {

    @Getter @Setter HashTable hashTable;

    @Getter @Setter int ID;

    @Getter @Setter private QueryGraph probingSubgraph;
    private String queryVertexToHash;
    @Getter private int buildHashIdx;
    @Getter private int hashedTupleLen;

    /**
     * Constructs a {@link Build} object.
     *
     * @param inSubgraph is the subgraph matched by the input tuples.
     * @param queryVertexToHash is the query vertex to hash on.
     * @param buildHashIdx is the index of the query vertex in the build tuple to hash on.
     */
    Build(QueryGraph inSubgraph, String queryVertexToHash, int buildHashIdx) {
        this.inSubgraph = inSubgraph;
        this.hashedTupleLen = inSubgraph.getNumVertices() - 1;
        this.outTupleLen = inSubgraph.getNumVertices();
        this.queryVertexToHash = queryVertexToHash;
        this.buildHashIdx = buildHashIdx;
        this.name = "HASH ON (" + queryVertexToHash + ")";
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        if (null == this.probeTuple) {
            this.probeTuple = probeTuple;
            this.hashTable.allocateInitialMemory(graph.getHighestVertexId());
        }
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() {
        hashTable.insertTuple(probeTuple);
    }

    /**
     * @see Operator#setDataflowsDescriptors()
     */
    public void setDataflowsDescriptors() {}

    /**
     * @see Operator#isSameAs(Operator)
     */
    public boolean isSameAs(Operator operator) {
        if (!(operator instanceof Build)) {
            return false;
        }
        var build = (Build) operator;
        return this == operator ||
            (inSubgraph.isIsomorphicTo(build.getInSubgraph()) && prev.isSameAs(build.prev));
    }

    /**
     * @see Operator#copy(boolean)
     */
    public Build copy(boolean isThreadSafe) {
        var build = new Build(inSubgraph, queryVertexToHash, buildHashIdx);
        build.prev = prev.copy(isThreadSafe);
        build.prev.setNext(build);
        build.probingSubgraph = probingSubgraph;
        build.setID(ID);
        return build;
    }
}
