package ca.waterloo.dsg.graphflow.query;

import lombok.var;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A setAdjListSortOrder of {@link QueryGraph}s.
 */
public class QueryGraphSet {

    private Map<String, List<QueryGraph>> queryGraphs = new LinkedHashMap<>();

    /**
     * @param queryGraph The {@link QueryGraph} to append to the set.
     */
    public void add(QueryGraph queryGraph) {
        var encoding = queryGraph.getEncoding();
        queryGraphs.putIfAbsent(encoding, new ArrayList<>());
        queryGraphs.get(encoding).add(queryGraph);
    }

    /**
     * Checks if an isomorphic query graph is in the setAdjListSortOrder.
     *
     * @param queryGraph The {@link QueryGraph} to check.
     * @return True if an isomorphic {@link QueryGraph} is in the set. False, otherwise.
     */
    public boolean contains(QueryGraph queryGraph) {
        var queryGraphs = this.queryGraphs.get(queryGraph.getEncoding());
        if (null == queryGraphs) {
            return false;
        }
        for (var otherQueryGraph : queryGraphs) {
            if (queryGraph.isIsomorphicTo(otherQueryGraph)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return The sorted {@link QueryGraph} setAdjListSortOrder.
     */
    public Set<QueryGraph> getQueryGraphSet() {
        var queryGraphSet = new LinkedHashSet<QueryGraph>();
        for (var encoding : queryGraphs.keySet()) {
            queryGraphSet.addAll(queryGraphs.get(encoding));
        }
        return queryGraphSet;
    }
}
