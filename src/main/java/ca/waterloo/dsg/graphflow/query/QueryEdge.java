package ca.waterloo.dsg.graphflow.query;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Represents a query edge.
 */
public class QueryEdge implements Serializable {

    @Getter private String fromVertex;
    @Getter private String toVertex;
    @Getter @Setter private short fromType;
    @Getter @Setter private short toType;
    @Getter @Setter private short label;

    /**
     * Constructs a {@link QueryEdge} object.
     *
     * @param fromVertex is the from query vertex of the query edge.
     * @param toVertex is the to query vertex of the query edge.
     * @param fromType is the from query vertex type.
     * @param toType is the to query vertex type.
     * @param label is the query edge label.
     */
    public QueryEdge(String fromVertex, String toVertex, short fromType, short toType,
        short label) {
        this.fromVertex = fromVertex;
        this.toVertex = toVertex;
        this.fromType = fromType;
        this.toType = toType;
        this.label = label;
    }

    /**
     * Constructs a {@link QueryEdge} object.
     *
     * @param fromQVertex is the from query vertex of the query edge.
     * @param toQVertex is the to query vertex of the query edge.
     */
    public QueryEdge(String fromQVertex, String toQVertex) {
        this(fromQVertex, toQVertex, (short) 0  /* fromType */, (short) 0 /* toType */,
            (short) 0 /* label */);
    }
}
