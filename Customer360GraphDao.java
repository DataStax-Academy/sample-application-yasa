package com.datastax.yasa.customer360;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addE;


import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.driver.dse.graph.GraphNode;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphStatement;
import com.datastax.driver.dse.graph.SimpleGraphStatement;
import com.datastax.dse.graph.api.DseGraph;
import com.datastax.dse.graph.api.TraversalBatch;
import com.datastax.yasa.dse.dao.GraphDao;
import com.datastax.yasa.ui.model.VizJsGraph;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.ImmutableMap;


/**
 * Leveraging on gremlin queries and VizJS to display c360.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
@Repository
public class Customer360GraphDao extends GraphDao {

    /** Loger for that class. */
    protected Logger LOGGER = LoggerFactory.getLogger(Customer360GraphDao.class);
    
    private static final String GRAPHNAME_C360 = "graphc360";
    
    // GRAPH ENTITIES
    public static final String VERTEX_CLUSTER               = "cluster";
    public static final String VERTEX_CUSTOMER              = "customer";
    public static final String VERTEX_VEHICULE              = "vehicule";
    public static final String VERTEX_CONTRACT              = "contract";
    public static final String EDGE_CONTAINS_CUSTOMER       = "containsCustomer";
    
    // COLUMNS
    public static final String COL_cluster_id               = "cluster_id";
    public static final String COL_cluster_size             = "cluster_size";
    public static final String COL_confidence_level         = "confidence_level";
    public static final String COL_golden_company_name      = "golden_company_name";
    public static final String COL_golden_company_reg_no    = "golden_company_reg_no";
    public static final String COL_golden_customer_name     = "golden_customer_name";
    public static final String COL_golden_customer_type     = "golden_customer_type";
    public static final String COL_golden_display_name      = "golden_display_name";
    public static final String COL_golden_dob               = "golden_dob";
    public static final String COL_golden_firstname         = "golden_firstname";
    public static final String COL_golden_surname           = "golden_surname";
    public static final String COL_cd_si_ext                = "cd_si_ext";
    public static final String COL_src_customer_id          = "src_customer_id";
    public static final String COL_customer_type            = "customer_type";
    public static final String COL_firstname                = "firstname";
    public static final String COL_surname                  = "surname";
    public static final String COL_company_reg_no           = "company_reg_no";
    public static final String COL_company_name             = "company_name";
    public static final String COL_dob                      = "dob";
    
    // DATE
    public static final String dob_date_format              = "yyyy-mm-dd";
    public static final SimpleDateFormat dob_date_formatter = new SimpleDateFormat(dob_date_format);
    
    /**
     * Create Graph.
     */
    public void createGraphC360() {
        createGraph(GRAPHNAME_C360, true);
        executeGremlinFile(GRAPHNAME_C360, new File("src/test/resources/c360-graph.ddl"));
    }
    
    /**
     * Import Data into Graph using Batch Traversal API 
     */
    public void loadClustersAndCustomers(File csvFile, char separator) {
        try {
            CsvMapper mapper = new CsvMapper();
            CsvSchema schema = CsvSchema.emptySchema().withHeader().withColumnSeparator(separator);
            MappingIterator<Map<String, String>> iterator = mapper.readerFor(Map.class).with(schema).readValues(csvFile);
            long start = System.currentTimeMillis();
            TraversalBatch batch = DseGraph.batch();
            while (iterator.hasNext()) {
               Map <String, String > csvLine = iterator.next();
               batch.add(parseClusterVertexFromCsvLine(csvLine));
               batch.add(parseCustomerVertexFromCsvLine(csvLine));
               batch.add(parseClusterCustomerEdgesFromCsvLine(csvLine));
            }
            dseSession.executeGraph(batch.asGraphStatement().setGraphName(GRAPHNAME_C360));
            LOGGER.info("Executed in {} millis", System.currentTimeMillis() - start);
            System.out.println("END");
        } catch (Exception e) {
            throw new IllegalStateException("Cannot execute import ", e);
        }
    }
    
    private GraphTraversal<Object, Edge> parseClusterCustomerEdgesFromCsvLine(Map<String, String> csvLine) {
        return addE(EDGE_CONTAINS_CUSTOMER)
                .from(DetachedVertex.build()
                        .setId(ImmutableMap.of(COL_cluster_id, csvLine.get(COL_cluster_id), "~label", VERTEX_CLUSTER))
                        .setLabel(VERTEX_CLUSTER).create())
                .to(DetachedVertex.build()
                        .setId(ImmutableMap.of(COL_src_customer_id, csvLine.get(COL_src_customer_id) , "~label", VERTEX_CUSTOMER))
                        .setLabel(VERTEX_CUSTOMER).create());
    }
    
    private GraphTraversal<Object, Vertex> parseCustomerVertexFromCsvLine(Map<String, String> csvLine)
    throws ParseException {
        GraphTraversal<Object, Vertex> customerVertex = addV(VERTEX_CUSTOMER)
                .property(COL_src_customer_id, csvLine.get(COL_src_customer_id));
        if (csvLine.containsKey(COL_cd_si_ext)) {
            customerVertex.property(COL_cd_si_ext, csvLine.get(COL_cd_si_ext));
        }
        if (csvLine.containsKey(COL_company_name)) {
            customerVertex.property(COL_company_name, csvLine.get(COL_company_name));
        }        
        if (csvLine.containsKey(COL_company_reg_no)) {
            customerVertex.property(COL_company_reg_no, csvLine.get(COL_company_reg_no));
        }
        if (csvLine.containsKey(COL_customer_type)) {
            customerVertex.property(COL_customer_type, csvLine.get(COL_customer_type));
        }
        if (csvLine.containsKey(COL_dob) && StringUtils.hasLength(csvLine.get(COL_dob))) {
            Date javaUtilDate = dob_date_formatter.parse(csvLine.get(COL_dob));
            LocalDate localDate = javaUtilDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            customerVertex.property(COL_dob, localDate);
        }
        if (csvLine.containsKey(COL_firstname)) {
            customerVertex.property(COL_firstname, csvLine.get(COL_firstname));
        }
        if (csvLine.containsKey(COL_surname)) {
            customerVertex.property(COL_surname, csvLine.get(COL_surname));
        }
        return customerVertex;
    }
    
    private GraphTraversal<Object, Vertex> parseClusterVertexFromCsvLine(Map<String, String> csvLine)
    throws ParseException {
        GraphTraversal<Object, Vertex> clusterVertex = addV(VERTEX_CLUSTER).property(COL_cluster_id, csvLine.get(COL_cluster_id))
                .property(COL_cluster_size, csvLine.get(COL_cluster_size))
                .property(COL_confidence_level, csvLine.get(COL_confidence_level));
        if (csvLine.containsKey(COL_golden_company_name)) {
            clusterVertex.property(COL_golden_company_name, csvLine.get(COL_golden_company_name));
        }
        if (csvLine.containsKey(COL_golden_company_reg_no)) {
            clusterVertex.property(COL_golden_company_reg_no, csvLine.get(COL_golden_company_reg_no));
        }
        if (csvLine.containsKey(COL_golden_customer_name)) {
            clusterVertex.property(COL_golden_customer_name, csvLine.get(COL_golden_customer_name));
        }
        if (csvLine.containsKey(COL_golden_customer_type)) {
            clusterVertex.property(COL_golden_customer_type, csvLine.get(COL_golden_customer_type));
        }
        if (csvLine.containsKey(COL_golden_display_name)) {
            clusterVertex.property(COL_golden_display_name, csvLine.get(COL_golden_display_name));
        }
        if (csvLine.containsKey(COL_golden_dob) && StringUtils.hasLength(csvLine.get(COL_golden_dob))) {
            Date javaUtilDate = dob_date_formatter.parse(csvLine.get(COL_golden_dob));
            LocalDate localDate = javaUtilDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            clusterVertex.property(COL_golden_dob, localDate);
        }
        if (csvLine.containsKey(COL_golden_firstname)) {
            clusterVertex.property(COL_golden_firstname, csvLine.get(COL_golden_firstname));
        }
        if (csvLine.containsKey(COL_golden_surname)) {
            clusterVertex.property(COL_golden_surname, csvLine.get(COL_golden_surname));
        }
        return clusterVertex;
    }
    
    /**
     * Retrieve list of cluster Id.
     * 
     * @param graphName
     *      current GraphName
     * @return
     *      list of cluster ids.
     */
    @SuppressWarnings("unchecked")
    public Map < String, String > listClusterIds(String graphName) {
        Assert.hasText(graphName, "'graphName' is required here");
        final String queryGetClusterdIds = "g.V().hasLabel('cluster').valueMap('cluster_id', 'golden_display_name');";   
        GraphStatement graphStatement = new SimpleGraphStatement(queryGetClusterdIds).setGraphName(graphName);
        GraphResultSet gResult = dseSession.executeGraph(graphStatement);
        // no paging here guys, can be better for sure
        Map < String, String > mapOfClusters = new HashMap<>();
        for (GraphNode graphNode : gResult) {
            LinkedHashMap<String, Object> map = graphNode.as(LinkedHashMap.class);
            mapOfClusters.put(map.get("golden_display_name").toString(),
                              map.get("cluster_id").toString());
        }
        return mapOfClusters;
    }
    
    /**
     * Giving a golden customer id give me the underlying network
     * 
     * @param graphName
     *      current GraphName
     * @param clusterId
     *      current cluster identifier
     * @return
     *      edges and vertices to be displayed in UI>
     */
    public VizJsGraph loadClusterByClusterId(String graphName, String clusterId) {
        Assert.hasText(graphName, "'graphName' is required here");
        Assert.hasText(clusterId, "'clusterId' is required here");
        LOGGER.info("Load cluster '{}' on graph '{}'", clusterId, graphName);
        String querySearchForCluster = "g.V().has('cluster', 'cluster_id', '" + clusterId + "')"
                                          + ".emit().repeat(both().simplePath())"
                                          + ".times(4).dedup();";
        return executeGremlinQuery(graphName, querySearchForCluster, true);
    }
    
}
