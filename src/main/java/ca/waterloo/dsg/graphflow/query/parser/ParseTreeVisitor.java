package ca.waterloo.dsg.graphflow.query.parser;

import ca.waterloo.dsg.graphflow.grammar.GraphflowBaseVisitor;
import ca.waterloo.dsg.graphflow.grammar.GraphflowParser.EdgeContext;
import ca.waterloo.dsg.graphflow.grammar.GraphflowParser.GraphflowContext;
import ca.waterloo.dsg.graphflow.grammar.GraphflowParser.MatchPatternContext;
import ca.waterloo.dsg.graphflow.query.QueryEdge;
import ca.waterloo.dsg.graphflow.query.QueryGraph;
import ca.waterloo.dsg.graphflow.storage.KeyStore;
import lombok.var;

/**
 * This class implements the ANTLR4 methods used to traverse the parseAntlr tree and return a
 * {@link QueryGraph} object.
 */
public class ParseTreeVisitor extends GraphflowBaseVisitor<QueryGraph> {

    private KeyStore store;

    /**
     * Constructs a {@link ParseTreeVisitor} object.
     *
     * @param store The type and label key store.
     */
    ParseTreeVisitor(KeyStore store) {
        this.store = store;
    }

    @Override
    public QueryGraph visitGraphflow(GraphflowContext ctx) {
        var queryGraph = visitMatchPattern(ctx.matchPattern());
        if (null != ctx.LIMIT()) {
            queryGraph.setLimit(Integer.parseInt(ctx.Digits().getText()));
        }
        return queryGraph;
    }

    @Override
    public QueryGraph visitMatchPattern(MatchPatternContext ctx) {
        var queryGraph = new QueryGraph();
        for (int i = 0; i < ctx.edge().size(); i++) {
            visitEdge(ctx.edge(i), queryGraph);
        }
        for (var queryEdge : queryGraph.getEdges()) {
            queryEdge.setFromType(queryGraph.getVertexType(queryEdge.getFromVertex()));
            queryEdge.setToType(queryGraph.getVertexType(queryEdge.getToVertex()));
        }
        return queryGraph;
    }

    private void visitEdge(EdgeContext ctx, QueryGraph queryGraph) {
        var fromQVertex = ctx.vertex(0).variable().getText();
        var toQVertex = ctx.vertex(1).variable().getText();
        var queryEdge = new QueryEdge(fromQVertex, toQVertex);
        if (null != ctx.vertex(0).type()) {
            var fromType = ctx.vertex(0).type().variable().getText();
            queryEdge.setFromType(store.getTypeKeyAsShort(fromType));
        } else if (queryGraph.getVertexToTypeMap().containsKey(fromQVertex)) {
            queryEdge.setFromType(queryGraph.getVertexToTypeMap().get(fromQVertex));
        }
        if (null != ctx.vertex(1).type()) {
            var toType = ctx.vertex(1).type().variable().getText();
            queryEdge.setToType(store.getTypeKeyAsShort(toType));
        } else if (queryGraph.getVertexToTypeMap().containsKey(toQVertex)) {
            queryEdge.setToType(queryGraph.getVertexToTypeMap().get(toQVertex));
        }
        if (null != ctx.label()) {
            var label = ctx.label().variable().getText();
            queryEdge.setLabel(store.getLabelKeyAsShort(label));
        }
        queryGraph.addEdge(queryEdge);
    }
}
