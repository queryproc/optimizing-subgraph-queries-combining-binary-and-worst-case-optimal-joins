package ca.waterloo.dsg.graphflow.planner.catalog.operator;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.plan.operator.extend.EI;
import ca.waterloo.dsg.graphflow.plan.operator.extend.Intersect;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Given a set of input tuples from the prev {@link Operator}, E/I extends the tuples by one query
 * vertex.
 */
public class IntersectCatalog extends Intersect implements Serializable {

    private boolean isAdjListSortedByType;
    private long lastIcost = 0;

    /**
     * @see EI#make(String, short, List, QueryGraph, QueryGraph, Map)
     */
    public IntersectCatalog(String toQVertex, short toType, List<AdjListDescriptor> ALDs,
        QueryGraph outSubgraph, QueryGraph inSubgraph, Map<String, Integer> outQVertexToIdxMap,
        boolean isAdjListSortedByType) {
        super(toQVertex, toType, ALDs, outSubgraph, inSubgraph, outQVertexToIdxMap);
        this.isAdjListSortedByType = isAdjListSortedByType;
    }

    /**
     * @see Operator#init(int[], Graph, KeyStore)
     */
    @Override
    public void init(int[] probeTuple, Graph graph, KeyStore store) {
        var type = toType;
        super.init(probeTuple, graph, store);
        toType = type;
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() throws LimitExceededException {
        //System.out.println("process here:\n");
        if (1 == ALDs.size()) {
        // intersect the adjacency lists and setAdjListSortOrder the output vertex values.
        adjListsToCache[0][probeTuple[vertexIdxToCache[0]]].setNeighbourIds(
            labelsOrToTypesToCache[0], outNeighbours);
        icost += outNeighbours.endIdx - outNeighbours.startIdx;  // 有多少项匹配, 从 probetuple 出节点出发, type相同
        } else {
        // intersect the adjacency lists and setAdjListSortOrder the output vertex values.
        Neighbours temp;
        if (cachingType == CachingType.NONE || !isIntersectionCached()) {
            adjListsToCache[0][probeTuple[vertexIdxToCache[0]]].setNeighbourIds(
                labelsOrToTypesToCache[0], initNeighbours);
            lastIcost = initNeighbours.endIdx - initNeighbours.startIdx;
            lastIcost += adjListsToCache[1][probeTuple[vertexIdxToCache[1]]].intersect(
                labelsOrToTypesToCache[1], initNeighbours, cachedNeighbours);

            // 如果 tovertex 有 type 限制, 要再扫描一遍, 因为 totype 只有一个, 所以下面不需要再次扫描
            if (toType != KeyStore.ANY) {
                var currEndIdx = 0;
                for (var i = cachedNeighbours.startIdx; i < cachedNeighbours.endIdx; i++) {
                    if (vertexTypes[cachedNeighbours.Ids[i]] == toType) {
                        cachedNeighbours.Ids[currEndIdx++] = cachedNeighbours.Ids[i];
                    }
                }
                cachedNeighbours.endIdx = currEndIdx;
            }

            for (var i = 2; i < adjListsToCache.length; i++) {
                temp = cachedNeighbours;
                cachedNeighbours = tempNeighbours;
                tempNeighbours = temp;  // temp 和 cached 互换
                lastIcost += adjListsToCache[i][probeTuple[vertexIdxToCache[i]]].intersect(
                    labelsOrToTypesToCache[i], tempNeighbours, cachedNeighbours);
            }
        }
        switch (cachingType) {
            case NONE:
            case FULL_CACHING:
                icost += lastIcost;
                outNeighbours = cachedNeighbours;
                break;
            case PARTIAL_CACHING:
                icost += adjLists[0][probeTuple[vertexIdx[0]]].intersect(
                    labelsOrToTypes[0], cachedNeighbours, outNeighbours);
                for (int i = 1; i < adjLists.length; i++) {
                    temp = outNeighbours;
                    outNeighbours = tempNeighbours;
                    tempNeighbours = temp;
                    icost += adjLists[i][probeTuple[vertexIdx[i]]].intersect(
                        labelsOrToTypes[i], tempNeighbours, outNeighbours);
                }
                break;
        }
            //System.out.println(this.getName() + "   icost = " + icost);
        }

        for (var idx = outNeighbours.startIdx; idx < outNeighbours.endIdx; idx++) {
            probeTuple[outIdx] = outNeighbours.Ids[idx];
            numOutTuples++;
            if (isAdjListSortedByType) {
                next[0].processNewTuple();
            } else {
                next[vertexTypes[probeTuple[outIdx]]].processNewTuple();
            }
        }
    }

    /**
     * @see Operator#copy()
     */
    @Override
    public IntersectCatalog copy() {
        var copy = new IntersectCatalog(toQueryVertex, toType, ALDs, outSubgraph, inSubgraph,
            outQVertexToIdxMap, isAdjListSortedByType);
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
