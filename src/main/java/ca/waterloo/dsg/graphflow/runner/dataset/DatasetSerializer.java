package ca.waterloo.dsg.graphflow.runner.dataset;

import ca.waterloo.dsg.graphflow.runner.AbstractRunner;
import ca.waterloo.dsg.graphflow.runner.ArgsFactory;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.GraphFactory;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.var;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Loads the given .csv file and saves it in a serialized format in the given directory.
 */
public class DatasetSerializer extends AbstractRunner {

    private static final Logger logger = LogManager.getLogger(DatasetSerializer.class);

    public static void main(String[] args) throws IOException {
        // If the user asks for help, enforce it over the required options.
        if (isAskingHelp(args, getCommandLineOptions())) {
            return;
        }

        var cmdLine = parseCmdLine(args, getCommandLineOptions());
        if (null == cmdLine) {
            logger.info("could not parse all the program arguments.");
            return;
        }

        Graph graph;
        var store = new KeyStore();
        try {
            var csvEdgesFile = cmdLine.getOptionValue(ArgsFactory.INPUT_FILE_EDGES);
            var edgesSeparator = ",";
            if (cmdLine.hasOption(ArgsFactory.EDGES_FILE_SEPARATOR)) {
                edgesSeparator = cmdLine.getOptionValue(ArgsFactory.EDGES_FILE_SEPARATOR);
            }
            if (cmdLine.hasOption(ArgsFactory.INPUT_FILE_VERTICES)) {
                var csvVerticesFile = cmdLine.getOptionValue(ArgsFactory.INPUT_FILE_VERTICES);
                String verticesSeparator = ",";
                if (cmdLine.hasOption(ArgsFactory.VERTICES_FILE_SEPARATOR)) {
                    verticesSeparator = cmdLine.getOptionValue(ArgsFactory.
                        VERTICES_FILE_SEPARATOR);
                }
                graph = new GraphFactory().make(csvVerticesFile, verticesSeparator,
                    csvEdgesFile, edgesSeparator, store);
            } else {
                graph = new GraphFactory().make(csvEdgesFile, edgesSeparator, store);
            }
        } catch (IOException e) {
            logger.info("Could not load the csv input graph data.");
            return;
        }

        graph.setUndirected(cmdLine.hasOption(ArgsFactory.UNDIRECTED));

        // Serialize the data and save the files in the given output directory.
        var outputDirectory = sanitizeDirStrAndMkdirIfNeeded(cmdLine.getOptionValue(
            ArgsFactory.SERIALIZE_OUTPUT));
        try {
            store.serialize(outputDirectory);
            graph.serialize(outputDirectory);
        } catch (IOException e) {
            logger.error("Error in serialization: ", e.getMessage());
        }
    }

    /**
     * @return The {@link Options} required by the {@link DatasetSerializer}.
     */
    private static Options getCommandLineOptions() {
        var options = new Options();                               // ArgsFactory.
        options.addOption(ArgsFactory.getInputFileEdges());        // INPUT_FILE_EDGES        -e
        options.addOption(ArgsFactory.getOutputDirOption());       // SERIALIZE_OUTPUT        -o
        options.addOption(ArgsFactory.getEdgesFileSeparator());    // EDGES_FILE_SEPARATOR    -m
        options.addOption(ArgsFactory.getInputFileVertices());     // INPUT_FILE_VERTICES     -v
        options.addOption(ArgsFactory.getVerticesFileSeparator()); // VERTICES_FILE_SEPARATOR -n
        options.addOption(ArgsFactory.getIsGraphUndirected());     // UNDIRECTED             -u
        return options;
    }
}
