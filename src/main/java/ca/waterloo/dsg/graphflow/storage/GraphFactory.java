package ca.waterloo.dsg.graphflow.storage;

import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.var;
import org.antlr.v4.runtime.misc.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Constructs a {@link Graph} object from CSV file and binary serialized data.
 * TODO: optimize the loading by reducing the I/O time.
 */
public class GraphFactory {

    /**
     * Constructs a {@link Graph} object from binary serialized data.
     *
     * @param directory is the directory to deserialize binary data from.
     * @return the constructed {@link Graph} object.
     * @throws IOException if stream to file cannot be written to or closed.
     * @throws ClassNotFoundException if the object read is from input stream is not found.
     */
    @SuppressWarnings("unchecked") // casting.
    public Graph make(String directory) throws IOException, ClassNotFoundException {
        // load the initial fields to construct the graph.
        var fwdAdjLists = (SortedAdjList[]) IOUtils.deserializeObj(directory + "fwdAdjLists");
        var bwdAdjLists = (SortedAdjList[]) IOUtils.deserializeObj(directory + "bwdAdjLists");
        var highestVertexId = (Integer) IOUtils.deserializeObj(directory + "highestVertexId");
        // create the graph object and setAdjListSortOrder its vertices and basic statistics.
        var graph = new Graph(fwdAdjLists, bwdAdjLists, highestVertexId);
        graph.setVertexIds((int[]) IOUtils.deserializeObj(directory + "vertexIds"));
        graph.setVertexTypes((short[]) IOUtils.deserializeObj(directory + "vertexTypes"));
        graph.setVertexTypeOffsets((int[]) IOUtils.deserializeObj(directory + "vertexTypeOffsets"));
        graph.setNumEdges((int) IOUtils.deserializeObj(directory + "numEdges"));
        graph.setLabelOrToTypeToNumEdges((int[])
            IOUtils.deserializeObj(directory + "labelOrToTypeToNumEdges"));
        graph.setLabelOrTypeToLargestFwdAdjListSize((int[])
            IOUtils.deserializeObj(directory + "labelOrTypeToLargestFwdAdjListSize"));
        graph.setLabelOrTypeToLargestBwdAdjListSize((int[])
            IOUtils.deserializeObj(directory + "labelOrTypeToLargestBwdAdjListSize"));
        graph.setEdgeKeyToNumEdgesMap((Map<Long, Integer>)
            IOUtils.deserializeObj(directory + "edgeKeyToNumEdgesMap"));
        graph.setLabelAndToTypeToPercentageMap((Map<Integer, Integer>)
            IOUtils.deserializeObj(directory + "labelAndToTypeToPercentageMap"));
        graph.setFromTypeAndLabelToPercentageMap((Map<Integer, Integer>)
            IOUtils.deserializeObj(directory + "fromTypeAndLabelToPercentageMap"));
        graph.setAdjListSortedByType((boolean) IOUtils.deserializeObj(
            directory + "isAdjListSortedByType"));
        graph.setUndirected((boolean) IOUtils.deserializeObj(directory + "isUndirected"));
        return graph;
    }

    /**
     * Constructs a {@link Graph} object from the edges csv file.
     *
     * @param verticesCSVFile is the vertices to load.
     * @param verticesCSVSeparator is the separator between various columns.
     * @param edgesCSVFile is the edges csv file to load.
     * @param edgesCSVSeparator is the separator between various columns.
     * @param store is the vertex types and edge labelsOrToTypes key store.
     * @return the constructed {@link Graph} object.
     * @throws IOException if stream to file cannot be written to or closed.
     */
    public Graph make(String verticesCSVFile, String verticesCSVSeparator, String edgesCSVFile,
        String edgesCSVSeparator, KeyStore store) throws IOException {
        var graph = new Graph();
        loadVertices(verticesCSVFile, verticesCSVSeparator, store, graph);
        loadEdges(edgesCSVFile, edgesCSVSeparator, store, graph);
        graph.setEdgeCountsAndLargestAdjListSizes(store);
        return graph;
    }

    /**
     * Constructs a {@link Graph} object from the edges csv file.
     *
     * @param edgesCSVFile is the edges csv file to load data from.
     * @param separator is the separator between various columns.
     * @param store is the vertex types and edge labelsOrToTypes key store.
     * @return the constructed {@link Graph} object.
     * @throws IOException if stream to file cannot be written to or closed.
     */
    public Graph make(String edgesCSVFile, String separator, KeyStore store) throws IOException {
        var graph = new Graph();
        loadEdges(edgesCSVFile, separator, store, graph);
        graph.setEdgeCountsAndLargestAdjListSizes(store);
        return graph;
    }

    private void loadVertices(String file, String separator, KeyStore store, Graph graph)
        throws IOException {
        var offsets = insertTypesAndGetOffsets(file, separator, store);
        var numVertices = offsets[offsets.length - 1];
        var vertexIds = new int[numVertices];
        var vertexTypes = new short[numVertices];
        var currIdxByType = new int[offsets.length - 1];
        var reader = new BufferedReader(new FileReader(file));
        var highestVertexId = Integer.MIN_VALUE;
        var line = reader.readLine();
        while (null != line) {
            var row = line.split(separator);
            var vertexId = Integer.parseInt(row[0]);
            if (vertexId > highestVertexId) {
                highestVertexId = vertexId;
            }
            var vertexType = store.getTypeKeyAsShort(row[1]);
            vertexIds[offsets[vertexType] + currIdxByType[vertexType]] = vertexId;
            currIdxByType[vertexType] += 1;
            vertexTypes[vertexId] = vertexType;
            line = reader.readLine();
        }
        graph.setHighestVertexId(highestVertexId);
        graph.setVertexIds(vertexIds);
        graph.setVertexTypes(vertexTypes);
        graph.setVertexTypeOffsets(offsets);
    }

    private void loadEdges(String file, String separator, KeyStore store, Graph graph)
        throws IOException {
        var highestVertexId = insertLabelsAndGetHighestVertexId(file, separator, store);
        var sortByType = store.getNextLabelKey() == 1  /* key 0 only used -> single label.  */
                      && store.getNextTypeKey()   > 1; /* at least 2 vertex key types used. */
        if (-1 == graph.getHighestVertexId()) {
            graph.setHighestVertexId(highestVertexId);
            var vertexIds = new int[graph.getHighestVertexId() + 1];
            for (var i = 0; i < graph.getHighestVertexId() + 1; i++) {
                vertexIds[i] = i;
            }
            graph.setVertexIds(vertexIds);
            graph.setVertexTypes(new short[graph.getHighestVertexId() + 1]);
            graph.setVertexTypeOffsets(new int[] {0, graph.getHighestVertexId() + 1});
            // all vertices have type '0' so we insert it.
            store.insertTypeKeyIfNeeded("0");
        }

        var adjListsMetadata = getAdjListMetadata(file, separator, store, sortByType, graph);
        var numVertices = highestVertexId + 1;
        var fwdAdjLists = new SortedAdjList[numVertices];
        var bwdAdjLists = new SortedAdjList[numVertices];
        var fwdAdjListCurrIdx = new HashMap<Integer, int[]>(numVertices);
        var bwdAdjListCurrIdx = new HashMap<Integer, int[]>(numVertices);
        var offsetSize = sortByType ? store.getNextTypeKey() : store.getNextLabelKey();
        for (var vertexId = 0; vertexId < numVertices; vertexId++) {
            fwdAdjLists[vertexId] = new SortedAdjList(adjListsMetadata.a.get(vertexId));
            fwdAdjListCurrIdx.put(vertexId, new int[offsetSize]);
            bwdAdjLists[vertexId] = new SortedAdjList(adjListsMetadata.b.get(vertexId));
            bwdAdjListCurrIdx.put(vertexId, new int[offsetSize]);
        }

        var reader = new BufferedReader(new FileReader(file));
        var line = reader.readLine();
        while (null != line) {
            var row = line.split(separator);
            var fromVertex = Integer.parseInt(row[0]);
            var toVertex = Integer.parseInt(row[1]);
            var fromTypeOrLabel = sortByType ? graph.getVertexTypes()[fromVertex] :
                store.getLabelKeyAsShort(row[2]);
            var toTypeOrLabel = sortByType ? graph.getVertexTypes()[toVertex] :
                store.getLabelKeyAsShort(row[2]);
            var idx = fwdAdjListCurrIdx.get(fromVertex)[toTypeOrLabel];
            var offset = adjListsMetadata.a.get(fromVertex)[toTypeOrLabel];
            fwdAdjListCurrIdx.get(fromVertex)[toTypeOrLabel] += 1;
            fwdAdjLists[fromVertex].setNeighbourId(toVertex, idx + offset);
            idx = bwdAdjListCurrIdx.get(toVertex)[fromTypeOrLabel];
            offset = adjListsMetadata.b.get(toVertex)[fromTypeOrLabel];
            bwdAdjListCurrIdx.get(toVertex)[fromTypeOrLabel] += 1;
            bwdAdjLists[toVertex].setNeighbourId(fromVertex, idx + offset);
            line = reader.readLine();
        }
        for (var vertexId = 0; vertexId < numVertices; vertexId++) {
            fwdAdjLists[vertexId].sort();
            bwdAdjLists[vertexId].sort();
        }
        graph.setFwdAdjLists(fwdAdjLists);
        graph.setBwdAdjLists(bwdAdjLists);
    }

    private int[] insertTypesAndGetOffsets(String file, String separator, KeyStore store)
        throws IOException {
        var reader = new BufferedReader(new FileReader(file));
        var TypeToCountMap = new HashMap<Short, Integer>();
        var line = reader.readLine();
        while (null != line) {
            var typeAsStr = line.split(separator)[1];
            store.insertTypeKeyIfNeeded(typeAsStr);
            var type = store.getTypeKeyAsShort(typeAsStr);
            TypeToCountMap.putIfAbsent(type, 0);
            TypeToCountMap.put(type, TypeToCountMap.get(type) + 1);
            line = reader.readLine();
        }
        var offsets = new int[store.getNextTypeKey() + 1];
        for (var key : TypeToCountMap.keySet()) {
            if (key < store.getNextTypeKey() - 1) {
                offsets[key + 1] = TypeToCountMap.get(key);
            }
            offsets[store.getNextTypeKey()] += TypeToCountMap.get(key);
        }
        for (var i = 1; i < offsets.length - 1; i++) {
            offsets[i] += offsets[i - 1];
        }
        return offsets;
    }

    private int insertLabelsAndGetHighestVertexId(String csvFile, String separator, KeyStore store)
        throws IOException {
        var reader = new BufferedReader(new FileReader(csvFile));
        var line = reader.readLine();
        var highestVertexId = Integer.MIN_VALUE;
        while (null != line) {
            var row = line.split(separator);
            var fromVertex = Integer.parseInt(row[0]);
            var toVertex = Integer.parseInt(row[1]);
            store.insertLabelKeyIfNeeded(row[2]);
            if (fromVertex > highestVertexId) {
                highestVertexId = fromVertex;
            }
            if (toVertex > highestVertexId) {
                highestVertexId = toVertex;
            }
            line = reader.readLine();
        }
        return highestVertexId;
    }

    private Pair<Map<Integer, int[]>, Map<Integer, int[]>> getAdjListMetadata(String file,
        String separator, KeyStore store, boolean sortByType, Graph graph) throws IOException {
        Map<Integer, int[]> fwdAdjListMetadata = new HashMap<>();
        Map<Integer, int[]> bwdAdjListMetadata = new HashMap<>();
        var nextLabelOrType = (sortByType ? store.getNextTypeKey() : store.getNextLabelKey());
        for (int i = 0; i <= graph.getHighestVertexId(); i++) {
            fwdAdjListMetadata.put(i, new int[nextLabelOrType + 1]);
            bwdAdjListMetadata.put(i, new int[nextLabelOrType + 1]);
        }
        var reader = new BufferedReader(new FileReader(file));
        var line = reader.readLine();
        while (null != line) {
            String[] row = line.split(separator);
            var fromVertex = Integer.parseInt(row[0]);
            var toVertex = Integer.parseInt(row[1]);
            if (sortByType) {
                var fromType = graph.getVertexTypes()[fromVertex];
                var toType = graph.getVertexTypes()[toVertex];
                fwdAdjListMetadata.get(fromVertex)[toType + 1] += 1;
                bwdAdjListMetadata.get(toVertex)[fromType + 1] += 1;
            } else {
                var label = store.getLabelKeyAsShort(row[2]);
                fwdAdjListMetadata.get(fromVertex)[label + 1] += 1;
                bwdAdjListMetadata.get(toVertex)[label + 1] += 1;
            }
            line = reader.readLine();
        }
        for (var offsets : fwdAdjListMetadata.values()) {
            for (var i = 1; i < offsets.length - 1; i++) {
                offsets[nextLabelOrType] += offsets[i];
                offsets[i] += offsets[i - 1];
            }
        }
        for (var offsets : bwdAdjListMetadata.values()) {
            for (var i = 1; i < offsets.length - 1; i++) {
                offsets[nextLabelOrType] += offsets[i];
                offsets[i] += offsets[i - 1];
            }
        }
        return new Pair<>(fwdAdjListMetadata, bwdAdjListMetadata);
    }
}
