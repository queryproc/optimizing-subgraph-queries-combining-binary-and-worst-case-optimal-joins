package ca.waterloo.dsg.graphflow.plan.operator;

import ca.waterloo.dsg.graphflow.storage.Graph.Direction;
import lombok.Getter;

import java.io.Serializable;

/**
 * An adjacency list descriptor consists of the following:
 * (1) A from variable indicating the vertex in the query graph that is being extended from.
 * (2) An index indicating the vertex value position in the processing tuple from which we extend.
 * (3) A direction which indicates whether to extend from fwd or bwd adj list.
 * (4) An edge label.
 */
public class AdjListDescriptor implements Serializable {

    @Getter private String fromQueryVertex;
    @Getter private int vertexIdx;
    @Getter private Direction direction;
    @Getter private short label;

    /**
     * Constructs an {@link AdjListDescriptor} object.
     *
     * @param fromQueryVertex is the from variable to extend from.
     * @param vertexIdx is the index in the tuple indicating the vertex from which we extend.
     * @param dir is the direction of extension.
     * @param label is the edge label.
     */
    public AdjListDescriptor(String fromQueryVertex, int vertexIdx, Direction dir, short label) {
        this.fromQueryVertex = fromQueryVertex;
        this.vertexIdx = vertexIdx;
        this.direction = dir;
        this.label = label;
    }

    @Override
    public String toString() {
        return (-1 != vertexIdx ? "vertexIdx: " + vertexIdx : "") +
            ", fromQueryVertex: " + fromQueryVertex +
            ", direction: " + direction.name() + ", and " +
            (label == -1 ? "no label" : "internal label: " + label + ".");
    }
}
