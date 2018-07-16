package com.datastax.yasa.dse.utils;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.dse.DseCluster.Builder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.driver.dse.graph.GraphProtocol;
import com.datastax.dse.graph.api.DseGraph;
import com.datastax.dse.graph.api.TraversalBatch;
import com.google.common.collect.ImmutableMap;

/**
 * Invoke Java driver API FROM Spark. 
 * 
 * Datastax provides both Spark Cassandra Connector and GraphFrame to connect to GRAPH. To use
 * those capabilities remotely you need BYOS (Bring your own spark) and have spark with same
 * version as DSE.
 * 
 * For older ENV wih spark 1.x we need to work with the Java Driver itself to connect to DSE to use graph 
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class SparkDseDriversBridge {
    
    /** Loger for that class. */
    protected static Logger LOGGER = LoggerFactory.getLogger(SparkDseDriversBridge.class);
    
    // Configuration file
    private static final String KEY_CASSANDRA_CONTACTPOINT      = "dse.cassandra.contactPoints";
    private static final String KEY_CASSANDRA_PORT              = "dse.cassandra.port";
    private static final String KEY_CASSANDRA_USERNAME          = "dse.cassandra.username";
    private static final String KEY_CASSANDRA_PASSWORD          = "dse.cassandra.password";
    private static final String KEY_GRAPH_TIMEOUT               = "dse.graph.timeout";
    private static final String KEY_GRAPH_NAME                  = "dse.graph.name";
    
    // Info Graph
    private static final String VERTEX_CLUSTER               = "cluster";
    private static final String VERTEX_CUSTOMER              = "customer";
    private static final String EDGE_CONTAINS_CUSTOMER       = "containsCustomer";
    private static final String COL_cluster_id               = "cluster_id";
    private static final String COL_cluster_size             = "cluster_size";
    private static final String COL_confidence_level         = "confidence_level";
    private static final String COL_golden_company_name      = "golden_company_name";
    private static final String COL_golden_company_reg_no    = "golden_company_reg_no";
    private static final String COL_golden_customer_name     = "golden_customer_name";
    private static final String COL_golden_customer_type     = "golden_customer_type";
    private static final String COL_golden_display_name      = "golden_display_name";
    private static final String COL_golden_dob               = "golden_dob";
    private static final String COL_golden_firstname         = "golden_firstname";
    private static final String COL_golden_surname           = "golden_surname";
    private static final String COL_cd_si_ext                = "cd_si_ext";
    private static final String COL_src_customer_id          = "src_customer_id";
    private static final String COL_customer_type            = "customer_type";
    private static final String COL_firstname                = "firstname";
    private static final String COL_surname                  = "surname";
    private static final String COL_company_reg_no           = "company_reg_no";
    private static final String COL_company_name             = "company_name";
    private static final String COL_dob                      = "dob";
    public static final String dob_date_format               = "yyyy-mm-dd";
    public static final SimpleDateFormat dob_date_formatter  = new SimpleDateFormat(dob_date_format);
     
    /** Attributes for instances. */
    private Properties config;
    private DseSession dseSession;
    private String graphName;
    
    /** Singleton Pattern. */
    private static SparkDseDriversBridge instance;
    
    /** Singleton Pattern. */
    public static synchronized SparkDseDriversBridge getInstance() {
        if (instance != null) {
            instance = new SparkDseDriversBridge();
            instance.parseConfigFile("dse.properties");
            instance.initConnectionToDse();
        }
        return instance;
    }
    
    /** Hide default constructor. */
    private SparkDseDriversBridge() {}
    
    /** Initialization on Class loading. */
    static { 
        getInstance();
    }
    
    /**
     * 
     * rddRecord
     * 
     * 
     * @param rddRecord
     */
    void saveToDseGraph_ClusterCustomer(Map < String, String > rddRecord) {
        if (rddRecord != null) {
            TraversalBatch batch = DseGraph.batch();
            batch.add(parseClusterVertexFromCsvLine(rddRecord));
            batch.add(parseCustomerVertexFromCsvLine(rddRecord));
            batch.add(parseClusterCustomerEdgesFromCsvLine(rddRecord));
            dseSession.executeGraph(batch.asGraphStatement().setGraphName(graphName));
        }
    }
    
    private void initConnectionToDse() {
        int    dsePort         = Integer.parseInt(config.getProperty(KEY_CASSANDRA_PORT, "9042"));
        String dseUsername     = config.getProperty(KEY_CASSANDRA_USERNAME);
        String dsePassword     = config.getProperty(KEY_CASSANDRA_PASSWORD);
        int    dseGraphTimeout = Integer.parseInt(config.getProperty(KEY_GRAPH_TIMEOUT, "30000"));
        List<String> contactPoints = Arrays.asList(config.getProperty(KEY_CASSANDRA_CONTACTPOINT).split(","));
        
        LOGGER.info("Initializing connection to DSE Cluster");
        Builder clusterConfig = new Builder();
        LOGGER.info(" + DSE Contact Points : {}" , contactPoints);
        contactPoints.stream().forEach(clusterConfig::addContactPoint);
        LOGGER.info(" + DSE DB Port : {}", dsePort);
        clusterConfig.withPort(dsePort);
        if (dseUsername != null && !"".equals(dseUsername)) {
            AuthProvider cassandraAuthProvider = new DsePlainTextAuthProvider(dseUsername, dsePassword);
            clusterConfig.withAuthProvider(cassandraAuthProvider);
            LOGGER.info(" + With username  : {}", dseUsername);
        }
        GraphOptions graphOption = new GraphOptions();
        graphOption.setReadTimeoutMillis(dseGraphTimeout);
        graphOption.setGraphSubProtocol(GraphProtocol.GRAPHSON_2_0);
        clusterConfig.withGraphOptions(graphOption);
        dseSession = clusterConfig.build().connect();
    }
    
    /**
     * Load Configuration File.
     */
    private void parseConfigFile(String fileName) {
        InputStream in = SparkDseDriversBridge.class.getResourceAsStream(fileName);
        if (in == null) {
            in =  SparkDseDriversBridge.class.getClassLoader().getResourceAsStream(fileName);
        }
        if (in == null) {
            in =  Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        }
        if (in == null) {
            throw new IllegalStateException("Cannot load file " + fileName + " please check that file exists");
        }
        Properties propertiesConfigDse = new Properties();
        try {
            propertiesConfigDse.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot parse file" + fileName + " please check file content.");
        }
        graphName = config.getProperty(KEY_GRAPH_NAME);
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
    
    private GraphTraversal<Object, Vertex> parseCustomerVertexFromCsvLine(Map<String, String> csvLine) {
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
        if (csvLine.containsKey(COL_dob)) {
            customerVertex.property(COL_dob, csvLine.get(COL_dob));
        }
        if (csvLine.containsKey(COL_firstname)) {
            customerVertex.property(COL_firstname, csvLine.get(COL_firstname));
        }
        if (csvLine.containsKey(COL_surname)) {
            customerVertex.property(COL_surname, csvLine.get(COL_surname));
        }
        return customerVertex;
    }
    
    private GraphTraversal<Object, Vertex> parseClusterVertexFromCsvLine(Map<String, String> csvLine) {
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
        if (csvLine.containsKey(COL_golden_dob)) {
            clusterVertex.property(COL_golden_dob, csvLine.get(COL_golden_dob));
        }
        if (csvLine.containsKey(COL_golden_firstname)) {
            clusterVertex.property(COL_golden_firstname, csvLine.get(COL_golden_firstname));
        }
        if (csvLine.containsKey(COL_golden_surname)) {
            clusterVertex.property(COL_golden_surname, csvLine.get(COL_golden_surname));
        }
        return clusterVertex;
    }
    
}
