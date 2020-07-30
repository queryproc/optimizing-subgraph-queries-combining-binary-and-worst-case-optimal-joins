package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.util.IOUtils;
import lombok.var;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * The base class for shared functionality between Runner classes.
 */
public abstract class AbstractRunner {

    private static final Logger logger = LogManager.getLogger(AbstractRunner.class);

    /**
     * sanitizes the string directory from user input and ensures it ends with character '/'.
     * Creates the directory if not already created in the file system.
     *
     * @param userDefinedDirectory The output directory from command line.
     */
    protected static String sanitizeDirStrAndMkdirIfNeeded(String userDefinedDirectory)
        throws IOException {
        String outputDirectory = sanitizeDirStr(userDefinedDirectory);
        IOUtils.mkdirs(outputDirectory);
        return outputDirectory;
    }

    protected static String sanitizeDirStr(String userDefinedDirectory) {
        String outputDirectory = userDefinedDirectory;
        if (!outputDirectory.equals("") && !outputDirectory.endsWith("/")) {
            outputDirectory += "/";
        }
        return outputDirectory;
    }

    /**
     * @param args The supplied command-line arguments.
     * @param options The setAdjListSortOrder of {@link Options} to parse the args.
     *
     * @return A {@link CommandLine} object providing access to the parsed args given the options.
     */
    protected static CommandLine parseCmdLine(String[] args, Options options) {
        var cmdLineParser = new DefaultParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = cmdLineParser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Error parsing cmd line - " + e.getMessage());
        }
        return cmdLine;
    }

    protected static boolean isAskingHelp(String[] args, Options options) {
        try {
            if (isAskingHelp(args)) {
                HelpFormatter fmt = new HelpFormatter();
                fmt.printHelp("Help", options);
                return true;
            }
        } catch (ParseException e) {
            // ignore the parsing error as it is due to not using the proper options.
        }
        return false;
    }

    private static boolean isAskingHelp(String[] args) throws ParseException {
        Option helpOption = ArgsFactory.getHelpOption();
        Options options = new Options();
        options.addOption(helpOption);
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd.hasOption(helpOption.getOpt());
    }
}
