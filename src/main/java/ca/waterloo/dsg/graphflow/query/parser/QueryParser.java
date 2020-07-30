package ca.waterloo.dsg.graphflow.query.parser;

import ca.waterloo.dsg.graphflow.grammar.GraphflowLexer;
import ca.waterloo.dsg.graphflow.grammar.GraphflowParser;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.var;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Converts a raw query string into a {@code ParsedQuery} object.
 */
public class QueryParser {

    private static final Logger logger = LogManager.getLogger(QueryParser.class);

    /**
     * Parses the user query to obtain a {@link QueryGraph} object.
     *
     * @param query is the user query.
     * @return The parsed {@link QueryGraph}.
     */
    public static QueryGraph parse(String query, KeyStore store) {
        QueryGraph queryGraph;
        try {
            queryGraph = parseAntlr(query + ";", store);
        } catch (ParseCancellationException e) {
            logger.debug("ERROR parsing: " + e.getMessage());
            return null;
        }
        if (queryGraph == null) {
            logger.debug("queryGraph not parsed properly.");
            return null;
        }
        return queryGraph;
    }

    private static QueryGraph parseAntlr(String query, KeyStore store)
        throws ParseCancellationException {
        var lexer = new GraphflowLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();   // Remove default listeners first.
        lexer.addErrorListener(AntlrErrorListener.INSTANCE);

        var parser = new GraphflowParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();   // Remove default listeners first.
        parser.addErrorListener(AntlrErrorListener.INSTANCE);

        try {
            var visitor = new ParseTreeVisitor(store);
            return visitor.visit(parser.graphflow() /* parseTree */);
        } catch (Exception e) {
            throw new ParseCancellationException(e.getMessage());
        }
    }
}
