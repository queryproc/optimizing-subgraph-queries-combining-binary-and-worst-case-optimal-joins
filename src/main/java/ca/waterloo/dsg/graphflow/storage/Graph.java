package ca.waterloo.dsg.graphflow.storage;

import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.var;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The input graph data stored.
 */
public class Graph {

    private static final Logger logger = LogManager.getLogger(Graph.class);

    /**
     * Used to identify the edge direction in the graph representation.
     */
    public enum Direction {
        Fwd /* forward  */,
        Bwd /* backward */
    }

    // vertex Ids indexed by type and random access to vertex types.
    @Getter @Setter private int[] vertexIds;
    @Getter @Setter private short[] vertexTypes;
    @Getter @Setter private int[] vertexTypeOffsets;
    // Adjacency lists containing the neighbour vertex IDs sorted by ID.
    @Getter @Setter private SortedAdjList[] fwdAdjLists;
    @Getter @Setter private SortedAdjList[] bwdAdjLists;
    // Graph metadata.
    @Getter @Setter private int highestVertexId = -1;
    @Getter @Setter private int numEdges;
    @Setter private int[] labelOrToTypeToNumEdges;
    @Setter private int[] labelOrTypeToLargestFwdAdjListSize;
    @Setter private int[] labelOrTypeToLargestBwdAdjListSize;
    @Getter @Setter private Map<Long, Integer> edgeKeyToNumEdgesMap;
    @Getter @Setter private Map<Integer, Integer> labelAndToTypeToPercentageMap;
    @Getter @Setter private Map<Integer, Integer> fromTypeAndLabelToPercentageMap;
    @Getter @Setter private boolean isUndirected;
    @Getter @Setter private boolean isAdjListSortedByType;

    /**
     * Constructs a {@link Graph} object.
     */
     public Graph() {}

    /**
     * Constructs a {@link Graph} object.
     *
     * @param fwdAdjLists are the forward adjacency lists.
     * @param bwdAdjLists are the backward adjacency lists.
     * @param highestVertexId is the highest vertex ID.
     */
    public Graph(SortedAdjList[] fwdAdjLists, SortedAdjList[] bwdAdjLists, int highestVertexId) {
        this.fwdAdjLists = fwdAdjLists;
        this.bwdAdjLists = bwdAdjLists;
        this.highestVertexId = highestVertexId;
    }

    /**
     * @param fromType is the from query vertex type.
     * @param toType is the to query vertex type.
     * @param label is the edge label.
     * @return The number of edges.
     */
    public int getNumEdges(short fromType, short toType, short label) {
        if (fromType == KeyStore.ANY && toType == KeyStore.ANY) {
            return labelOrToTypeToNumEdges[label];
        } else if (fromType != KeyStore.ANY && toType != KeyStore.ANY) {
            return edgeKeyToNumEdgesMap.get(getEdgeKey(fromType, toType, label));
        } else if (fromType != KeyStore.ANY) {
            return fromTypeAndLabelToPercentageMap.get(getEdgeKey(fromType, label));
        }
        return labelAndToTypeToPercentageMap.get(getEdgeKey(label, toType));
    }

    /**
     * @param labelOrToType is the edge label.
     * @param direction is the direction of extension as forward or backward.
     * @return The largest adjacency list size.
     */
    public int getLargestAdjListSize(short labelOrToType, Direction direction) {
        if (Direction.Fwd == direction) {
            return labelOrTypeToLargestFwdAdjListSize[labelOrToType];
        } else {
            return labelOrTypeToLargestBwdAdjListSize[labelOrToType];
        }
    }

    /**
     * @param store is the vertex types and edge labelsOrToTypes key store.
     */
    void setEdgeCountsAndLargestAdjListSizes(KeyStore store) {
        // set the largest adjacency list sizes for forward and backward directions per label.
        isAdjListSortedByType = store.getNextLabelKey() == 1  /*key 0 only used -> single label.*/
                             && store.getNextTypeKey()   > 1; /*at least 2 vertex key types used.*/
        var numLabelsOrToTypes = isAdjListSortedByType ?
            store.getNextTypeKey() : store.getNextLabelKey();
        labelOrToTypeToNumEdges = new int[numLabelsOrToTypes];
        labelOrTypeToLargestFwdAdjListSize = new int[numLabelsOrToTypes];
        labelOrTypeToLargestBwdAdjListSize = new int[numLabelsOrToTypes];
        for (var vertexId = 0; vertexId <= highestVertexId; vertexId++) {
            numEdges += fwdAdjLists[vertexId].size();
            for (short labelOrToType = 0; labelOrToType < numLabelsOrToTypes; labelOrToType++) {
                var adjListSize = fwdAdjLists[vertexId].size(labelOrToType);
                labelOrToTypeToNumEdges[labelOrToType] += adjListSize;
                if (adjListSize > labelOrTypeToLargestFwdAdjListSize[labelOrToType]) {
                    labelOrTypeToLargestFwdAdjListSize[labelOrToType] = adjListSize;
                }
            }
            for (short labelOrToType = 0; labelOrToType < numLabelsOrToTypes; labelOrToType++) {
                var adjListSize = bwdAdjLists[vertexId].size(labelOrToType);
                if (adjListSize > labelOrTypeToLargestBwdAdjListSize[labelOrToType]) {
                    labelOrTypeToLargestBwdAdjListSize[labelOrToType] = adjListSize;
                }
            }
        }
        // Set the edge keys (fromType-label-toType), (fromType-label), and (label-toType) to
        // the percentage of number of edges.
        edgeKeyToNumEdgesMap = new HashMap<>();
        labelAndToTypeToPercentageMap = new HashMap<>();
        fromTypeAndLabelToPercentageMap = new HashMap<>();
        var numVertices = highestVertexId + 1;
        for (short fromType = 0; fromType < store.getNextTypeKey(); fromType++) {
            for (short toType = 0; toType < store.getNextTypeKey(); toType++) {
                for (short label = 0; label < store.getNextLabelKey(); label++) {
                    var edge = getEdgeKey(fromType, toType, label);
                    edgeKeyToNumEdgesMap.putIfAbsent(edge, 0);
                    var labelAndToType = getEdgeKey(label, toType);
                    labelAndToTypeToPercentageMap.putIfAbsent(labelAndToType, 0);
                    var fromTypeAndLabel = getEdgeKey(fromType, label);
                    fromTypeAndLabelToPercentageMap.putIfAbsent(fromTypeAndLabel, 0);
                }
            }
        }
        for (var fromVertex = 0; fromVertex < numVertices; fromVertex++) {
            var fromType = vertexTypes[fromVertex];
            var offsets = fwdAdjLists[fromVertex].getLabelOrTypeOffsets();
            if (isAdjListSortedByType) {
                short label = 0;
                for (short toType = 0; toType < offsets.length - 1; toType++) {
                    var numEdges = offsets[toType + 1] - offsets[toType];
                    addEdgeCount(fromType, toType, label, numEdges);
                }
            } else {
                var neighbours = fwdAdjLists[fromVertex].getNeighbourIds();
                for (short label = 0; label < offsets.length - 1; label++) {
                    for (var toIdx = offsets[label]; toIdx < offsets[label + 1]; toIdx++) {
                        var toType = vertexTypes[neighbours[toIdx]];
                        addEdgeCount(fromType, toType, label, 1);
                    }
                }
            }
        }
    }

    private void addEdgeCount(short fromType, short toType, short label, int numEdges) {
        var edge = getEdgeKey(fromType, toType, label);
        edgeKeyToNumEdgesMap.put(edge, edgeKeyToNumEdgesMap.get(edge) + numEdges);
        var labelAndToType = getEdgeKey(label, toType);
        labelAndToTypeToPercentageMap.put(labelAndToType,
            labelAndToTypeToPercentageMap.get(labelAndToType) + numEdges);
        var fromTypeAndLabel = getEdgeKey(fromType, label);
        fromTypeAndLabelToPercentageMap.put(fromTypeAndLabel,
            fromTypeAndLabelToPercentageMap.get(fromTypeAndLabel) + numEdges);
    }

    /**
     * @param fromType is the from query vertex type.
     * @param toType is the to query vertex type.
     * @param label is the query edge label.
     * @return the edge key.
     */
    public static long getEdgeKey(short fromType, short toType, short label) {
        return ((long) (fromType & 0xFFFF    ) << 48) |
               ((long) (label    & 0x0000FFFF) << 16) |
               ((long) (toType   & 0xFFFF    )      ) ;
    }

    /**
     * @param typeOrLabel is the from query vertex type.
     * @param typeOrLabel2 is the to query vertex type.
     * @return the edge key.
     */
    private int getEdgeKey(short typeOrLabel, short typeOrLabel2) {
        return ((typeOrLabel  & 0x0000FFFF) << 16) |
               ((typeOrLabel2 & 0xFFFF    )      ) ;
    }

    /**
     * Serializes the graph by persisting different fields into different files.
     *
     * @param directoryPath is the directory to which the graph's serialized objects are persisted.
     * @throws IOException if stream to file cannot be written to or closed.
     */
    public void serialize(String directoryPath) throws IOException {
        logger.info("Serializing the data graph.");
        IOUtils.serializeObjs(directoryPath, new Object[] {
            /* <filename , field to serialize> pair */
            "vertexIds", vertexIds,
            "vertexTypes", vertexTypes,
            "vertexTypeOffsets", vertexTypeOffsets,
            "highestVertexId", highestVertexId,
            "fwdAdjLists", fwdAdjLists,
            "bwdAdjLists", bwdAdjLists,
            "numEdges", numEdges,
            "isAdjListSortedByType", isAdjListSortedByType,
            "labelOrToTypeToNumEdges", labelOrToTypeToNumEdges,
            "labelOrTypeToLargestFwdAdjListSize", labelOrTypeToLargestFwdAdjListSize,
            "labelOrTypeToLargestBwdAdjListSize", labelOrTypeToLargestBwdAdjListSize,
            "edgeKeyToNumEdgesMap", edgeKeyToNumEdgesMap,
            "labelAndToTypeToPercentageMap", labelAndToTypeToPercentageMap,
            "fromTypeAndLabelToPercentageMap", fromTypeAndLabelToPercentageMap,
            "isUndirected", isUndirected
        });
    }
}
