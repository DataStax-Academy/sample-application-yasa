package com.datastax.yasa.dse.dao;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.Edge;
import com.datastax.driver.dse.graph.GraphNode;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphStatement;
import com.datastax.driver.dse.graph.SimpleGraphStatement;
import com.datastax.driver.dse.graph.Vertex;
import com.datastax.driver.dse.graph.VertexProperty;
import com.datastax.yasa.ui.model.GraphEdge;
import com.datastax.yasa.ui.model.GraphVertex;
import com.datastax.yasa.ui.model.VizJsGraph;

/**
 * Working with DSE GRAPH.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
@Repository
public class GraphDao {

    /** Loger for that class. */
    protected Logger LOGGER = LoggerFactory.getLogger(GraphDao.class);
   
    /** Hold Connectivity to DSE. */
    @Autowired
    protected DseSession dseSession;
    
    /**
     * Default constructor.
     */
    public GraphDao() {}
    
    /**
     * Allow explicit intialization for test purpose.
     */
    public GraphDao(DseSession dseSession) {
        this.dseSession     = dseSession;
    }
    
    /**
     * Simple Graph Creation.
     *
     * @param graphName
     *      target graphName
     */
    public void createGraph(String graphName, boolean ifNotExist) {
        Assert.hasText(graphName, "'graphName' is required here");
        String query = String.format("system.graph('%s')", graphName);
        if (ifNotExist) query += ".ifNotExists()";
        query += ".create()";
        LOGGER.info("Create graph with '{}'", query);
        dseSession.executeGraph(new SimpleGraphStatement(query).setSystemQuery());
    }
    
    /**
     * Execute gremling file.
     *
     * @param graphName
     * @param gremlinFile
     */
    public void executeGremlinFile(String graphName, File gremlinFile) {
        Assert.hasText(graphName, "'graphName' is required here");
        Assert.notNull(gremlinFile, "Gremlin file is not null");
        Assert.isTrue(gremlinFile.exists(), "Gremlin file does not exist");
        LOGGER.info("Processing Gremlin file: " + gremlinFile.getName());
        dseSession.getCluster().getConfiguration().getGraphOptions().setGraphName(graphName);
        try (Stream<String> stream = Files.lines(gremlinFile.toPath())) {
             stream.peek(line -> LOGGER.info(" + Executed. " + line))
                   .forEach(dseSession::executeGraph);
         } catch (IOException e) {
             LOGGER.error(" + An error occured ", e);
             throw new IllegalStateException("File has not been fully executed ", e);
         }
     }
   
    /**
     * Retrieve available graphs.
     *
     * @return
     *      graph names
     */
    public Set <String > listGraphNames() {
        GraphStatement gStatement =  new SimpleGraphStatement("system.graphs()").setSystemQuery();
        return dseSession.executeGraph(gStatement)
                         .all().stream().map(GraphNode::asString)
                         .collect(Collectors.toSet());
    }
    
    
    
    /**
     * On a vertex, list type of edges available
     *
     * @param graphName
     *      current graph name
     * @param vertexLabel
     *      vertex Label of type in your graph
     * @return
     *      list of vertices names
     */
    @SuppressWarnings("unchecked")
    public List < String > getEdgeNamesForVertex(String graphName, String vertexLabel) {
        Assert.hasText(graphName, "'graphName' is required here");
        Assert.hasText(vertexLabel, "'vertexLabel' is required here");
        
        String gremlinQuery = "def labels = graph.schema().traversal().V()";
        gremlinQuery+=".has('name', vertexLabel)\n";
        gremlinQuery+=".both('incident')\n.both('of')\n.values('name')";
        gremlinQuery+=".dedup().toList();\n[labels]";

        GraphStatement grapStatement = new SimpleGraphStatement(gremlinQuery)
                .set("vertexLabel", vertexLabel)
                .setGraphName(graphName);
        
        GraphResultSet graphResult   = dseSession.executeGraph(grapStatement);
        
        if (graphResult.iterator().hasNext()) {
            GraphNode node = graphResult.one();
            return node.as(ArrayList.class);
        }
        return new ArrayList<>();
    }
    
    /**
     * Load all Graph as a single query.
     * 
     * @param graphName
     *      target graohName
     * @return
     */
    public VizJsGraph loadGraph(String graphName) {
        Assert.hasText(graphName, "'graphName' is required here");
        return executeGremlinQuery(graphName, "g.V()", true);
    }
    
    /**
     * @param gremlinQuery
     */
    public VizJsGraph executeGremlinQuery(String graphName, String gremlinQuery, boolean populateEdges) {
        Assert.hasText(graphName, "'graphName' is required here");
        Assert.hasText(gremlinQuery, "'gremlinQuery' is required here");
        LOGGER.info("Executing query {} on graph {}", gremlinQuery, graphName);
        
        // QUERY FOR VERTICES
        GraphStatement graphStatement = new SimpleGraphStatement(gremlinQuery).setGraphName(graphName);
        GraphResultSet gras = dseSession.executeGraph(graphStatement);
        
        // MAPPING AS VERTICES IN UI
        VizJsGraph vizGraph    = new VizJsGraph();
        List<Object> vertexIds = new ArrayList<>();
        for (GraphNode gn : gras) {
            if (populateEdges && gn.isVertex()) vertexIds.add(gn.asVertex().getId());
            populateGraphVizJs(gn, vizGraph);
        }
        
        // QUERY FOR INTERMEDIATE EDGES
        if (populateEdges) {
            String queryNeightBoor = "g.V(ids.toArray()).outE().where(inV().id().is(within(ids)))";
            GraphStatement statementNeightBour = new SimpleGraphStatement(queryNeightBoor)
                    .set("ids", vertexIds)
                    .setGraphName(graphName);
            GraphResultSet res = dseSession.executeGraph(statementNeightBour);
            res.all().stream().forEach(gn -> populateGraphVizJs(gn, vizGraph));
        }
        return vizGraph;
    }
    
    /**
     * Utility to map {@link GraphNode} as UI Bean
     * @param gn
     *      graph node
     * @param graph
     *      target graph
     */
    private void populateGraphVizJs(GraphNode gn, VizJsGraph graph) {
        if (gn.isVertex()) {
            Vertex v = gn.asVertex();
            GraphVertex gv = new GraphVertex().id(v.getId().toString()).label(v.getLabel());
            
            if ("cluster".equals(v.getLabel())) {
                VertexProperty goldenDisplayName = v.getProperty("golden_display_name");
                if (goldenDisplayName != null) {
                    gv.setLabel(v.getProperty("golden_display_name").getValue().asString());
                }
                
            } else if ("customer".equalsIgnoreCase(v.getLabel())) {
                VertexProperty firstName = v.getProperty("firstname");
                VertexProperty surname = v.getProperty("surname");
                String label = "";
                if (surname != null) {
                    label+= surname.getValue().asString() + " ";
                }
                if (firstName != null) {
                    label+= firstName.getValue().asString();
                }
                if (!"".equals(label)) {
                    gv.setLabel(label);
                }
            } else if ("contract".equalsIgnoreCase(v.getLabel())) {
                VertexProperty sysAPL      = v.getProperty("sys_apl");
                VertexProperty agreementId = v.getProperty("agreement_id");
                String label = "";
                if (agreementId != null) {
                    label+= agreementId.getValue().asString();
                }
                if (sysAPL != null) {
                    label+= " (" + sysAPL.getValue().asString() + ")" ;
                }
                if (!"".equals(label)) {
                    gv.setLabel(label);
                }
            } else if ("vehicule".equalsIgnoreCase(v.getLabel())) {
                VertexProperty vin = v.getProperty("vin_no");
                String label = "";
                if (vin != null) {
                    label+= vin.getValue().asString();
                }
                if (!"".equals(label)) {
                    gv.setLabel(label);
                }
            }
            gv.setGroup(v.getLabel());
            graph.addVertex(gv);
            
        } else if (gn.isEdge()) {
            Edge e = gn.asEdge();
            graph.addEdge(new GraphEdge(e.getOutV().toString(), e.getInV().toString()));
         }
    }
    
    // ----- Custom Queries ---------
    
   
   
    
}
