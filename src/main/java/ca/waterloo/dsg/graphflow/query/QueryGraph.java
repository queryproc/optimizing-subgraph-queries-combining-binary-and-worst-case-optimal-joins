package ca.waterloo.dsg.graphflow.query;

import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.Getter;
import lombok.Setter;
import lombok.var;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import java.util.StringJoiner;

/**
 * A join query graph.
 */
public class QueryGraph implements Serializable {

    // Represents a map from a from to a to query vertex & the query edge between them.
    private Map<String, Map<String, QueryEdge>> vertexToEdgesMap = new HashMap<>();
    @Getter private Map<String, Short> vertexToTypeMap = new HashMap<>();
    private Map<String, int[]> vertexToDegMap = new HashMap<>();
    @Getter private List<QueryEdge> edges = new ArrayList<>();

    // Mapping iterator used to decide if two query graphs are isomorphic.
    private IsomorphismIterator it = null;
    private String encoding;
    @Getter @Setter private int limit;

    public short getVertexType(String queryVertex) {
        return vertexToTypeMap.get(queryVertex) == null ? 0 : vertexToTypeMap.get(queryVertex);
    }

    public void setVertexType(String queryVertex, Short toType) {
        vertexToTypeMap.put(queryVertex, toType);
        for (var qEdge : edges) {
            if (qEdge.getFromVertex().equals(queryVertex)) {
                qEdge.setFromType(toType);
            } else if (qEdge.getToVertex().equals(queryVertex)) {
                qEdge.setToType(toType);
            }
        }
    }

    public void addEdges(Collection<QueryEdge> queryEdges) {
        queryEdges.forEach(this::addEdge);
    }

    /**
     * Adds a relation to the {@link QueryGraph}. The relation is stored both in forward and
     * backward direction. There can be multiple relations with different directions and relation
     * types between two qVertices. A backward relation between fromQueryVertex and toQueryVertex is
     * represented by a {@link QueryEdge} from toQueryVertex to fromQueryVertex.
     *
     * @param qEdge The relation to be added.
     */
    public void addEdge(QueryEdge qEdge) {
        if (qEdge == null) {
            return;
        }
        // Get the vertex IDs.
        var fromQVertex = qEdge.getFromVertex();
        var toQVertex = qEdge.getToVertex();
        var fromType = qEdge.getFromType();
        var toType = qEdge.getToType();
        vertexToTypeMap.putIfAbsent(fromQVertex, KeyStore.ANY);
        vertexToTypeMap.putIfAbsent(toQVertex, KeyStore.ANY);
        if (KeyStore.ANY != fromType) {
            vertexToTypeMap.put(fromQVertex, fromType);
        }
        if (KeyStore.ANY != toType) {
            vertexToTypeMap.put(toQVertex, toType);
        }
        // Set the in and out degrees for each variable.
        if (!vertexToDegMap.containsKey(fromQVertex)) {
            int[] degrees = new int[2];
            vertexToDegMap.put(fromQVertex, degrees);
        }
        vertexToDegMap.get(fromQVertex)[0]++;
        if (!vertexToDegMap.containsKey(toQVertex)) {
            int[] degrees = new int[2];
            vertexToDegMap.put(toQVertex, degrees);
        }
        vertexToDegMap.get(toQVertex)[1]++;
        // Add fwd edge fromQVertex -> toQVertex to the vertexToEdgesMap.
        addQEdgeToQGraph(fromQVertex, toQVertex, qEdge);
        // Add bwd edge toQVertex <- fromQVertex to the vertexToEdgesMap.
        addQEdgeToQGraph(toQVertex, fromQVertex, qEdge);
        edges.add(qEdge);
    }

    /**
     * Adds the new {@link QueryEdge} to the query vertex to query edges map.
     *
     * @param fromQVertex is the from query vertex.
     * @param toQVertex is the to query vertex.
     * @param qEdge is the {@link QueryEdge} containing the query vertices and their types.
     */
    private void addQEdgeToQGraph(String fromQVertex, String toQVertex, QueryEdge qEdge) {
        vertexToEdgesMap.putIfAbsent(fromQVertex, new HashMap<>());
        vertexToEdgesMap.get(fromQVertex).putIfAbsent(toQVertex, qEdge);
    }

    /**
     * @param queryGraph is the {@link QueryGraph} to get isomorphic mappings for.
     * @return The mapping iterator.
     */
    public IsomorphismIterator getSubgraphMappingIterator(QueryGraph queryGraph) {
        if (null == it) {
            it = new IsomorphismIterator(new ArrayList<>(vertexToEdgesMap.keySet()));
        }
        it.init(queryGraph);
        return it;
    }

    /**
     * @return A copy of all the query vertices present in the query.
     */
    public Set<String> getQVertices() {
        return new HashSet<>(vertexToEdgesMap.keySet());
    }

    /**
     * @return The number of query vertices present in the query.
     */
    public int getNumVertices() {
        return vertexToEdgesMap.size();
    }

    /**
     * @return The number of query edges
     */
    public int getNumEdges() {
        return edges.size();
    }

    /**
     * @param vertex The from vertex.
     * @param neighborVertex The to vertex.
     * @return A list of {@link QueryEdge}s representing all the relations present between
     * {@code vertex} and {@code neighborVertex}.
     * @throws NoSuchElementException if the {@code vertex} is not present in the
     * {@link QueryGraph}.
     */
    public QueryEdge getEdge(String vertex, String neighborVertex) {
        if (!vertexToEdgesMap.containsKey(vertex) ||
            !vertexToEdgesMap.get(vertex).containsKey(neighborVertex)) {
            return null;
        }
        return vertexToEdgesMap.get(vertex).get(neighborVertex);
    }

    /**
     * @param fromVariables The set of {@code String} qVertices to get their to qVertices.
     * @return The set of {@code String} to qVertices.
     */
    public Set<String> getNeighbors(Collection<String> fromVariables) {
        var toVariables = new HashSet<String>();
        for (var fromVariable : fromVariables) {
            if (!vertexToEdgesMap.containsKey(fromVariable)) {
                throw new NoSuchElementException("The variable '" + fromVariable + "' is not " +
                    "present.");
            }
            toVariables.addAll(getNeighbors(fromVariable));
        }
        toVariables.removeAll(fromVariables);
        return toVariables;
    }

    /**
     * @param fromVariable The {@code String} variable to get its to qVertices.
     * @return The setAdjListSortOrder of {@code String} to qVertices.
     */
    public List<String> getNeighbors(String fromVariable) {
        if (!vertexToEdgesMap.containsKey(fromVariable)) {
            throw new NoSuchElementException("The variable '" + fromVariable + "' is not present.");
        }
        return new ArrayList<>(vertexToEdgesMap.get(fromVariable).keySet());
    }

    /**
     * @param vertex1 is one of the qVertices.
     * @param vertex2 is another query vertex of the qVertices.
     * @return {@code true} if there is a query edge between {@code vertex1} and {@code vertex2} in
     * any direction, {@code false} otherwise.
     */
    public boolean containsQueryEdge(String vertex1, String vertex2) {
        return vertexToEdgesMap.containsKey(vertex1) &&
            vertexToEdgesMap.get(vertex1).containsKey(vertex2);
    }

    /**
     * Check if the {@link QueryGraph} is isomorphic to another given {@link QueryGraph}.
     *
     * @param otherQueryGraph The other {@link QueryGraph} to check for isomorphism.
     * @return True, if the query graph and oQGraph are isomorphic. False, otherwise.
     */
    public boolean isIsomorphicTo(QueryGraph otherQueryGraph) {
        return null != otherQueryGraph && getNumVertices() == otherQueryGraph.getNumVertices() &&
            null != getSubgraphMappingIfAny(otherQueryGraph);
    }

    public Map<String, String> getIsomorphicMappingIfAny(QueryGraph otherQueryGraph) {
        if (!isIsomorphicTo(otherQueryGraph)) {
            return null;
        }
        return getSubgraphMappingIfAny(otherQueryGraph);
    }

    private Map<String, String> getSubgraphMappingIfAny(QueryGraph otherQueryGraph) {
        var it = getSubgraphMappingIterator(otherQueryGraph);
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    private Set<String> getQueryVertices() {
        return new HashSet<>(vertexToEdgesMap.keySet());
    }

    /**
     * @return A copy of the {@link QueryGraph} object.
     */
    public QueryGraph copy() {
        var queryGraphCopy = new QueryGraph();
        queryGraphCopy.addEdges(edges);
        return queryGraphCopy;
    }

    /**
     * @param queryEdgeToExclude The relation to exclude from the queryGraph being copied.
     * @return A copy of the {@link QueryGraph} object excluding the given relation.
     */
    public QueryGraph copyExcluding(QueryEdge queryEdgeToExclude) {
        var queryGraphCopy = new QueryGraph();
        for (QueryEdge queryEdge : edges) {
            if (!queryEdge.getFromVertex().equals(queryEdgeToExclude.getFromVertex()) ||
                    !queryEdge.getToVertex().equals(queryEdgeToExclude.getToVertex())) {
                queryGraphCopy.addEdge(queryEdge);
            }
        }
        return queryGraphCopy;
    }

    /**
     * @return a {@link String} encoding based on the degree of vertices and direction of edges that
     * can be used as a hash.
     */
    public String getEncoding() {
        if (encoding == null) {
            var queryVerticesEncoded = new String[vertexToEdgesMap.size()];
            int vertexIdx = 0;
            for (var fromVertex : vertexToEdgesMap.keySet()) {
                var encodingStrBuilder = new StringBuilder();
                for (var toVertex : vertexToEdgesMap.get(fromVertex).keySet()) {
                    var queryEdge = vertexToEdgesMap.get(fromVertex).get(toVertex);
                    if (fromVertex.equals(queryEdge.getFromVertex())) {
                        encodingStrBuilder.append("F");
                    } else {
                        encodingStrBuilder.append("B");
                    }
                }
                var encodingToSort = encodingStrBuilder.toString().toCharArray();
                Arrays.sort(encodingToSort);
                queryVerticesEncoded[vertexIdx++] = new String(encodingToSort);
            }
            Arrays.sort(queryVerticesEncoded);
            encoding = String.join(".", queryVerticesEncoded);
        }
        return encoding;
    }

    public String toStringWithTypesAndLabels() {
        var stringJoiner = new StringJoiner("");
        var isFirstQueryEdge = true;
        for (var fromVertex : vertexToEdgesMap.keySet()) {
            for (var toVertex : vertexToEdgesMap.get(fromVertex).keySet()) {
                var fromType = vertexToTypeMap.get(fromVertex);
                var toType = vertexToTypeMap.get(toVertex);
                var queryEdge = vertexToEdgesMap.get(fromVertex).get(toVertex);
                var label = queryEdge.getLabel();
                if (queryEdge.getFromVertex().equals(fromVertex)) {
                    if (isFirstQueryEdge) {
                        stringJoiner.add(String.format("(%s:%s)-[%s]->(%s:%s)", fromVertex,
                            fromType, label, toVertex, toType));
                        isFirstQueryEdge = false;
                    } else {
                        stringJoiner.add(String.format(", (%s:%s)-[%s]->(%s:%s)", fromVertex,
                            fromType, label, toVertex, toType));
                    }
                }
            }
        }
        return stringJoiner.toString();
    }

    @Override
    public String toString() {
        var stringJoiner = new StringJoiner("");
        var isFirstQueryEdge = true;
        for (var fromVertex : vertexToEdgesMap.keySet()) {
            for (var toVertex : vertexToEdgesMap.get(fromVertex).keySet()) {
                var queryEdge = vertexToEdgesMap.get(fromVertex).get(toVertex);
                if (queryEdge.getFromVertex().equals(fromVertex)) {
                    if (isFirstQueryEdge) {
                        stringJoiner.add(String.format("(%s)->(%s)", fromVertex, toVertex));
                        isFirstQueryEdge = false;
                    } else {
                        stringJoiner.add(String.format(", (%s)->(%s)", fromVertex, toVertex));
                    }
                }
            }
        }
        return stringJoiner.toString();
    }

    /**
     * An iterator over a set of possible mappings between two query graphs.
     */
    public class IsomorphismIterator implements Iterator<Map<String, String>>, Serializable {
        List<String> vertices;
        List<String> otherVertices;
        QueryGraph otherQueryGraph;

        boolean isNextComputed;
        Map<String, String> nextMapping;

        int[] otherVertexIdxMapping;
        Stack<String> currMapping = new Stack<>();
        List<List<String>> possibleVertexMappings = new ArrayList<>();

        /**
         * Constructs an iterator for variable mappings between two query graphs.
         *
         * @param queryVertices are the query vertices of 'this' query graph.
         */
        IsomorphismIterator(List<String> queryVertices) {
            this.vertices = queryVertices;
            this.nextMapping = new HashMap<>();
            for (var variable : this.vertices) {
                nextMapping.put(variable, "");
            }
        }

        /**
         * @see Iterator#next()
         */
        @Override
        public Map<String, String> next() {
            if (!hasNext()) {
                throw new UnsupportedOperationException("Has no nextMapping mappings.");
            }
            nextMapping.clear();
            for (int i = 0; i < otherVertices.size(); i++) {
                nextMapping.put(currMapping.get(i), otherVertices.get(i));
            }
            isNextComputed = false;
            return nextMapping;
        }

        /**
         * Initialized the iterator based on the other query graph to map vertices to.
         *
         * @param otherQueryGraph The {@link QueryGraph} to map vertices to.
         */
        void init(QueryGraph otherQueryGraph) {
            // OtherQueryGraph is expected to be isomorphic or a subgraph.
            this.otherQueryGraph = otherQueryGraph;
            this.otherVertices = new ArrayList<>(otherQueryGraph.getQueryVertices());
            if (otherVertices.size() > vertices.size()) {
                isNextComputed = true;
                return;
            }

            this.otherVertexIdxMapping = new int[otherVertices.size()];
            clearPossibleVertexMappings(otherVertices.size());
            currMapping.clear();
            // Find possible vertex mappings.
            for (int i = 0; i < otherVertices.size(); i++) {
                var otherVertex = otherVertices.get(i);
                var otherType = otherQueryGraph.getVertexType(otherVertex);
                var otherDeg = otherQueryGraph.vertexToDegMap.get(otherVertex);
                for (var j = 0; j < vertices.size(); j++) {
                    var vertex = vertices.get(j);
                    // Ensure the vertices, have the same type.
                    if (!vertexToTypeMap.get(vertex).equals(otherType)) {
                        continue;
                    }

                    // If the other query graph and this query graph have the same number of
                    // vertices, each other vertex has to exactly match the number of incoming
                    // and outgoing edges as that of a vertex in the query vertex.
                    // Else, the other query graph has less vertices than this query graph,
                    // therefore each other vertex has to have an equal or less number of
                    // incoming and outgoing edges.
                    var vertexDeg = vertexToDegMap.get(vertex);
                    if (Arrays.equals(otherDeg, vertexDeg) ||
                        (otherVertices.size() < vertices.size() &&
                            vertexDeg[0] >= otherDeg[0] && vertexDeg[1] >= otherDeg[1])) {
                        possibleVertexMappings.get(i).add(vertex);
                    }
                }
                // if the otherVertex has no possible vertex mappings, next is computed.
                if (0 == possibleVertexMappings.get(i).size()) {
                    isNextComputed = true;
                    return;
                }
            }
            isNextComputed = false;
            hasNext();
        }

        private void clearPossibleVertexMappings(int otherVerticesSize) {
            for (int i = 0; i < otherVerticesSize; i++) {
                if (possibleVertexMappings.size() <= i) {
                    possibleVertexMappings.add(new ArrayList<>());
                } else {
                    possibleVertexMappings.get(i).clear();
                }
            }
        }

        /**
         * @see Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            if (!isNextComputed) {
                if (currMapping.size() == otherVertices.size()) {
                    currMapping.pop();
                }
                Outer: do {
                    var nextIdx = currMapping.size();
                    var isNextMappingPossible = otherVertexIdxMapping[nextIdx] <
                        possibleVertexMappings.get(nextIdx).size();
                    if (nextIdx == 0 && isNextMappingPossible) {
                        currMapping.add(possibleVertexMappings.get(0).get(
                            otherVertexIdxMapping[0]++));
                    } else if (isNextMappingPossible) {
                        var newVertexMapping = possibleVertexMappings.get(nextIdx).get(
                            otherVertexIdxMapping[nextIdx]++);
                        var otherVertex = otherVertices.get(nextIdx);
                        for (var i = 0; i < currMapping.size(); i++) {
                            var prevVertexMapping = currMapping.get(i);
                            if (prevVertexMapping.equals(newVertexMapping)) {
                                continue Outer;
                            }
                            var prevOtherVertex = otherVertices.get(i);
                            QueryEdge edge = getEdge(newVertexMapping, prevVertexMapping);
                            var otherEdge = otherQueryGraph.getEdge(otherVertex, prevOtherVertex);
                            if (edge == null && otherEdge == null) {
                                continue;
                            }
                            if (edge == null) { // && otherEdge != null
                                continue Outer;
                            }
                            if (otherEdge == null) { // edge != null
                                continue;
                            }
                            if (edge.getLabel() != otherEdge.getLabel()) {
                                continue Outer;
                            }
                            if (!((edge.getFromVertex().equals(prevVertexMapping) &&
                                otherEdge.getFromVertex().equals(prevOtherVertex)) ||
                                (edge.getFromVertex().equals(newVertexMapping) &&
                                    otherEdge.getFromVertex().equals(otherVertex)))) {
                                continue Outer;
                            }
                        }
                        currMapping.add(newVertexMapping);
                    } else if (otherVertexIdxMapping[nextIdx] >=
                        possibleVertexMappings.get(nextIdx).size()) {
                        currMapping.pop();
                        otherVertexIdxMapping[nextIdx] = 0;
                    }
                    if (currMapping.size() == otherVertices.size()) {
                        break;
                    }
                } while (!(currMapping.size() == 0 &&
                    otherVertexIdxMapping[0] >= possibleVertexMappings.get(0).size()));
                isNextComputed = true;
            }
            return !currMapping.isEmpty();
        }
    }
}
