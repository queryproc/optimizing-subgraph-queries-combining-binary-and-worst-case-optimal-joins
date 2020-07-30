package ca.waterloo.dsg.graphflow.planner.catalog;

import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.var;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Constructs a {@link Graph} object from CSV and binary data.
 */
public class CatalogFactory {

    /**
     * Constructs a {@link Catalog} object from binary serialized data.
     *
     * @param directory is the directory to deserialize binary data from.
     * @return the constructed {@link Catalog} object.
     * @throws IOException if stream to file cannot be written to or closed.
     * @throws ClassNotFoundException if the object read is from input stream is not found.
     */
    @SuppressWarnings("unchecked") // casting.
    public Catalog make(String directory) throws IOException, ClassNotFoundException {
        var numSampledEdges = (int) IOUtils.deserializeObj(directory + "numSampledEdges");
        var icost = (Map<Integer, Map<String, Double>>) IOUtils.deserializeObj(
            directory + "icost_" + numSampledEdges);
        var cardinality = (Map<Integer, Map<String, Double>>) IOUtils.deserializeObj(
            directory + "selectivity_" + numSampledEdges);
        var inSubgraphs = (List<QueryGraph>) IOUtils.deserializeObj(directory + "inSubgraphs");
        var catalog = new Catalog(icost, cardinality, inSubgraphs);
        catalog.setAdjListSortedByType((boolean) IOUtils.deserializeObj(directory +
            "isAdjListSortedByType"));
        catalog.setNumSampledEdges(numSampledEdges);
        catalog.setMaxInputNumVertices((int) IOUtils.deserializeObj(directory +
            "maxInputNumVertices"));
        return catalog;
    }
}
