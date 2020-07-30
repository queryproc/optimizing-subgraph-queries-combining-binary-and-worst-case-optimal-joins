package ca.waterloo.dsg.graphflow.runner.plan;

import ca.waterloo.dsg.graphflow.plan.Workers;
import ca.waterloo.dsg.graphflow.planner.QueryPlanner;
import ca.waterloo.dsg.graphflow.planner.QueryPlannerBig;
import ca.waterloo.dsg.graphflow.planner.catalog.Catalog;
import ca.waterloo.dsg.graphflow.planner.catalog.CatalogFactory;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.query.parser.QueryParser;
import ca.waterloo.dsg.graphflow.runner.AbstractRunner;
import ca.waterloo.dsg.graphflow.runner.ArgsFactory;
import ca.waterloo.dsg.graphflow.storage.Graph;
import ca.waterloo.dsg.graphflow.storage.GraphFactory;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import ca.waterloo.dsg.graphflow.storage.KeyStoreFactory;
import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.var;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Runs a specific transform for a {@link QueryGraph} and logs the transform.
 */
public class OptimizerExecutor extends AbstractRunner {

    protected static final Logger logger = LogManager.getLogger(OptimizerExecutor.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        var startTimeLoading = System.nanoTime();
        var cmdLine = parseCmdLine(args, getCommandLineOptions());
        if (null == cmdLine) {
            return;
        }

        var inputDirectory = sanitizeDirStr(cmdLine.getOptionValue(ArgsFactory.INPUT_GRAPH_DIR));

        Graph graph;
        Catalog catalog;
        KeyStore store;
        try {
            graph = new GraphFactory().make(inputDirectory);
            catalog = new CatalogFactory().make(inputDirectory);
            store = new KeyStoreFactory().make(inputDirectory);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error in deserialization: " + e.getMessage());
            return;
        }

        var queryGraph = QueryParser.parse(cmdLine.getOptionValue(ArgsFactory.QUERY), store);
        if (null == queryGraph) {
            logger.error("An error occurred parsing the query graph.");
            return;
        }

        var numQVertices = queryGraph.getNumVertices();
        if (numQVertices >= 15 && numQVertices <= 25) {
            QueryPlannerBig.NUM_TOP_PLANS_KEPT = 5;
        } else if (numQVertices > 25) {
            QueryPlannerBig.NUM_TOP_PLANS_KEPT = 1;
        }
        var elapsedTimeLoading = IOUtils.getElapsedTimeInMillis(startTimeLoading);
        logger.info("Dataset loading run time: " + elapsedTimeLoading + " (ms)");

        var planner = queryGraph.getNumVertices() <= 8 ?
            new QueryPlanner(queryGraph, catalog, graph) :
            new QueryPlannerBig(queryGraph, catalog, graph);
        var beginTime = System.nanoTime();
        var queryPlan = planner.plan();
        var elapsedTime = IOUtils.getElapsedTimeInMillis(beginTime);
        if (!cmdLine.hasOption(ArgsFactory.OUTPUT_FILE)) {
            logger.info("Optimizer run time: " + elapsedTime + " (ms)");
        }
        if (cmdLine.hasOption(ArgsFactory.EXECUTE_PLAN)) {
            beginTime = System.nanoTime();
            // initialize and execute the query transform, get the output metrics and log it.
            var numThreads = !cmdLine.hasOption(ArgsFactory.NUM_THREADS) ? 1 /* single thread */ :
                Integer.parseInt(cmdLine.getOptionValue(ArgsFactory.NUM_THREADS));
            var workers = new Workers(queryPlan, numThreads);
            logger.info("Plan initialization before exec run time: " + elapsedTime + " (ms)");
            workers.init(graph, store);
            elapsedTime = IOUtils.getElapsedTimeInMillis(beginTime);
            workers.execute();
            if (cmdLine.hasOption(ArgsFactory.OUTPUT_FILE)) {
                IOUtils.log(cmdLine.getOptionValue(ArgsFactory.OUTPUT_FILE),
                    elapsedTime + "," + workers.getElapsedTime() + "\n");
            } else {
                var output = workers.getOutputLog();
                var outputSplit = output.split("Plan output:")[0].split(",");
                logger.info("Query execution run time: " + outputSplit[0] + " (ms)");
                logger.info("Number output tuples: " + outputSplit[1]);
                logger.info("Number intermediate tuples: " + outputSplit[2]);
                var line = "";
                for (var i = 4; i < outputSplit.length - 1; i++) {
                    line += outputSplit[i] + ", ";
                }
                line += outputSplit[outputSplit.length - 1].split("\n")[0];
                logger.info("Plan: " + line);
            }
        } else {
            logger.info("Plan output:" + queryPlan.getOutputLog());
        }
    }

    /**
     * @return The {@link Options} required by the {@link OptimizerExecutor}.
     */
    private static Options getCommandLineOptions() {
        var options = new Options();                                     // ArgsFactory.
        options.addOption(ArgsFactory.getInputGraphDirectoryOption());   // INPUT_GRAPH_DIR     -i
        options.addOption(ArgsFactory.getNotRequiredOutputFileOption()); // OUTPUT_FILE         -o
        options.addOption(ArgsFactory.getQueryOption());                 // QUERY               -q
        options.addOption(ArgsFactory.getNumberThreadsOption());         // NUM_THREADS         -t
        options.addOption(ArgsFactory.getExecuteOption());               // EXECUTE_PLAN        -e
        options.addOption(ArgsFactory.getEnableAdaptivityOption());      // ENABLE_ADAPTIVITY   -a
        return options;
    }
}
