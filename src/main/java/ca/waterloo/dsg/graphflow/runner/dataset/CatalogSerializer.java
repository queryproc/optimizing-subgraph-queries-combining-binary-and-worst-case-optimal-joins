package ca.waterloo.dsg.graphflow.runner.dataset;

import ca.waterloo.dsg.graphflow.planner.catalog.Catalog;
import ca.waterloo.dsg.graphflow.planner.catalog.CatalogPlans;
import ca.waterloo.dsg.graphflow.runner.AbstractRunner;
import ca.waterloo.dsg.graphflow.runner.ArgsFactory;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.GraphFactory;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.storage.KeyStoreFactory;
import lombok.var;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Loads the given dataset, resets the catalog and saves the catalog in a serialized format in the
 * given output directory.
 */
public class CatalogSerializer extends AbstractRunner {

    protected static final Logger logger = LogManager.getLogger(CatalogSerializer.class);

    public static void main(String[] args) throws InterruptedException {
        // If the user asks for help, enforce it over the required options.
        if (isAskingHelp(args, getCommandLineOptions())) {
            return;
        }

        var cmdLine = parseCmdLine(args, getCommandLineOptions());
        if (null == cmdLine) {
            logger.info("could not parse all the program arguments");
            return;
        }

        var maxInputNumVertices = cmdLine.hasOption(ArgsFactory.NUM_MAX_INPUT_VERTICES) ?
            Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.NUM_MAX_INPUT_VERTICES)) :
            CatalogPlans.DEF_MAX_INPUT_NUM_VERTICES;
        var numSampledEdges = cmdLine.hasOption(ArgsFactory.NUM_SAMPLED_EDGES) ?
            Integer.valueOf(cmdLine.getOptionValue(ArgsFactory.NUM_SAMPLED_EDGES)) :
            CatalogPlans.DEF_NUM_EDGES_TO_SAMPLE;

        // Run the plans and collect sampled estimates for i-cost and cardinality.
        var numThreads = cmdLine.hasOption(ArgsFactory.NUM_THREADS) ?
            Integer.valueOf(cmdLine.getOptionValue(ArgsFactory.NUM_THREADS)) : 1 /* default */;

        // Load the data from the given binary directory.
        var inputDirectory = sanitizeDirStr(cmdLine.getOptionValue(ArgsFactory.INPUT_GRAPH_DIR));
        Graph graph;
        KeyStore store;
        try {
            graph = new GraphFactory().make(inputDirectory);
            store = new KeyStoreFactory().make(inputDirectory);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error in deserialization: " + e.getMessage());
            return;
        }

        Catalog catalog = new Catalog(numSampledEdges, maxInputNumVertices);
        try {
            catalog.populate(graph, store, numThreads, inputDirectory + "/catalog.txt");
        } catch (IOException e) {
            logger.error("Error logging catalog in human readable format: " + e.getMessage());
        }
        try {
            catalog.serialize(inputDirectory);
        } catch (IOException e) {
            logger.error("Error in serializing the catalog: " + e.getMessage());
        }
    }

    /**
     * @return The {@link Options} required by the {@link CatalogSerializer}.
     */
    private static Options getCommandLineOptions() {
        var options = new Options();                                   // ArgsFactory.
        options.addOption(ArgsFactory.getInputGraphDirectoryOption()); // INPUT_GRAPH_DIR        -i
        options.addOption(ArgsFactory.getNumberEdgesToSampleOption()); // NUM_SAMPLED_EDGES      -n
        options.addOption(ArgsFactory.getMaxInputNumVerticesOption()); // NUM_MAX_INPUT_VERTICES -v
        options.addOption(ArgsFactory.getNumberThreadsOption());       // NUM_THREADS            -t
        return options;
    }
}
