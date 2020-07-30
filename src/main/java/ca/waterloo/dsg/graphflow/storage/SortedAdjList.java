package ca.waterloo.dsg.graphflow.storage;

import ca.waterloo.dsg.graphflow.plan.operator.extend.EI.Neighbours;
import lombok.Getter;
import lombok.Setter;
import lombok.var;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents the adjacency list of a vertex. Stores the IDs of the vertex's initNeighbours, the
 * types, and the IDs of edges that the vertex has to these initNeighbours in sorted arrays. Arrays
 * are sorted first by neighbour IDs and then by edge {@code short} type values.
 */
public class SortedAdjList implements Serializable {

    @Getter private int[] labelOrTypeOffsets;
    @Getter @Setter private int[] neighbourIds;

    /**
     * Constructs a {@link SortedAdjList} object.
     */
    public SortedAdjList(int[] offsets) {
        this.labelOrTypeOffsets = offsets;
        this.neighbourIds = new int[offsets[offsets.length - 1]];
    }

    /**
     *
     *
     * @param idx is the index of the neighbour id to return.
     * @return the ...
     */
    public int getNeighbourId(int idx) {
        return neighbourIds[idx];
    }

    /**
     * Sets a new neighbour with the given Id at a given index.
     *
     * @param neighbourId is the Id of the neighbour.
     * @param idx is the index of the neighbour in the internal array.
     */
    public void setNeighbourId(int neighbourId, int idx) {
        neighbourIds[idx] = neighbourId;
    }

    /**
     * @param labelOrType .
     * @param neighbours .
     */
    public void setNeighbourIds(short labelOrType, Neighbours neighbours) {
        neighbours.Ids = neighbourIds;
        neighbours.startIdx = labelOrTypeOffsets[labelOrType];
        neighbours.endIdx = labelOrTypeOffsets[labelOrType + 1];
    }

    /**
     * voila.
     *
     * @param labelOrType
     * @param someNeighbours
     * @param neighbours
     * @return
     */
    public int intersect(short labelOrType, Neighbours someNeighbours, Neighbours neighbours) {
        intersect(someNeighbours, neighbours, neighbourIds,
            labelOrTypeOffsets[labelOrType], labelOrTypeOffsets[labelOrType + 1]);
        return labelOrTypeOffsets[labelOrType + 1] - labelOrTypeOffsets[labelOrType];
    }

    private void intersect(Neighbours someNeighbours, Neighbours neighbours, int[] neighbourIds,
        int thisIdx, int thisIdxEnd) {
        neighbours.reset();
        var someNeighbourIds = someNeighbours.Ids;
        var someIdx = someNeighbours.startIdx;
        var someEndIdx = someNeighbours.endIdx;
        while (thisIdx < thisIdxEnd && someIdx < someEndIdx) {
            if (neighbourIds[thisIdx] < someNeighbourIds[someIdx]) {
                thisIdx++;
                while (thisIdx < thisIdxEnd &&
                        neighbourIds[thisIdx] < someNeighbourIds[someIdx]) {
                    thisIdx++;
                }
            } else if (neighbourIds[thisIdx] > someNeighbourIds[someIdx]) {
                someIdx++;
                while (someIdx < someEndIdx &&
                        neighbourIds[thisIdx] > someNeighbourIds[someIdx]) {
                    someIdx++;
                }
            } else {
                neighbours.Ids[neighbours.endIdx] = neighbourIds[thisIdx];
                neighbours.endIdx++;
                thisIdx++;
                someIdx++;
            }
        }
    }

    /**
     * Sorts each list of neighbour Ids of a particular label.
     */
    public void sort() {
        for (int i = 0; i < labelOrTypeOffsets.length - 1; i++) {
            Arrays.sort(neighbourIds, labelOrTypeOffsets[i], labelOrTypeOffsets[i + 1]);
        }
    }

    /**
     * @return the size of the adjacency list.
     */
    public int size() {
        return neighbourIds.length;
    }

    /**
     * @param labelOrType is the edge label or toVertex type.
     * @return the size of the adjacency list.
     */
    public int size(short labelOrType) {
        return labelOrTypeOffsets[labelOrType + 1] + labelOrTypeOffsets[labelOrType];
    }
}
