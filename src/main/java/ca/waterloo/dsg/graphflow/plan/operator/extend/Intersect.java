package ca.waterloo.dsg.graphflow.plan.operator.extend;

import ca.waterloo.dsg.graphflow.plan.operator.AdjListDescriptor;
import ca.waterloo.dsg.graphflow.plan.operator.Operator;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.var;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Given a set of input tuples from the prev {@link Operator}, E/I extends the tuples by one query
 * vertex.
 */
public class Intersect extends EI implements Serializable {

    /**
     * @see EI#make(String, short, List, QueryGraph, QueryGraph, Map)
     */
    protected Intersect(String toQVertex, short toType, List<AdjListDescriptor> ALDs,
        QueryGraph outSubgraph, QueryGraph inSubgraph, Map<String, Integer> outQVertexToIdxMap) {
        super(toQVertex, toType, ALDs, outSubgraph, inSubgraph);
        this.lastRepeatedVertexIdx = outTupleLen - 2;
        this.outQVertexToIdxMap = outQVertexToIdxMap;
        this.outIdx = this.outQVertexToIdxMap.get(toQVertex);
    }

    /**
     * @see Operator#processNewTuple()
     */
    @Override
    public void processNewTuple() throws LimitExceededException {
        Neighbours temp;
        if (cachingType == CachingType.NONE || !isIntersectionCached()) {
            adjListsToCache[0][probeTuple[vertexIdxToCache[0]]].setNeighbourIds(
                labelsOrToTypesToCache[0], initNeighbours);
            icost += (initNeighbours.endIdx - initNeighbours.startIdx);
            icost += adjListsToCache[1][probeTuple[vertexIdxToCache[1]]].intersect(
                labelsOrToTypesToCache[1], initNeighbours, cachedNeighbours);
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
                tempNeighbours = temp;
                icost += adjListsToCache[i][probeTuple[vertexIdxToCache[i]]].intersect(
                    labelsOrToTypesToCache[i], tempNeighbours, cachedNeighbours);
            }
        }
        switch (cachingType) {
            case NONE:
            case FULL_CACHING:
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
        // setAdjListSortOrder the initNeighbours ids in the output tuple.
        numOutTuples += (outNeighbours.endIdx - outNeighbours.startIdx);
        for (var idx = outNeighbours.startIdx; idx < outNeighbours.endIdx; idx++) {
            probeTuple[outIdx] = outNeighbours.Ids[idx];
            next[0].processNewTuple();
        }
    }

    /**
     * @see Operator#isSameAs(Operator)
     */
    @Override
    public boolean isSameAs(Operator operator) {
        if (!(operator instanceof Intersect)) {
            return false;
        }
        var intersect = (Intersect) operator;
        return
            this == intersect || (
                cachingType == intersect.getCachingType()              &&
                getALDsAsString().equals(intersect.getALDsAsString())  &&
                inSubgraph.isIsomorphicTo(intersect.getInSubgraph())   &&
                outSubgraph.isIsomorphicTo(intersect.getOutSubgraph()) &&
                prev.isSameAs(intersect.getPrev())
            );
    }

    /**
     * @see Operator#copy(boolean)
     */
    @Override
    public Intersect copy(boolean isThreadSafe) {
        var intersect = new Intersect(toQueryVertex, toType, ALDs, outSubgraph, inSubgraph,
            outQVertexToIdxMap);
        intersect.prev = prev.copy(isThreadSafe);
        intersect.prev.setNext(intersect);
        intersect.initCaching(intersect.prev.getLastRepeatedVertexIdx());
        return intersect;
    }
}
