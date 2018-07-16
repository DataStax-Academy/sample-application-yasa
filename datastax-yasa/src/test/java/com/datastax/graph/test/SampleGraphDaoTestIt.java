package com.datastax.graph.test;

import java.io.File;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.yasa.customer360.Customer360GraphDao;
import com.datastax.yasa.dse.conf.DseConfiguration;
import com.datastax.yasa.dse.dao.CassandraDao;
import com.datastax.yasa.dse.dao.GraphDao;


/**
 * Direct integartion test of DAO (no need for API)
 *
 * @author DataStax Evangelist Team
 */
@ContextConfiguration(classes={DseConfiguration.class, GraphDao.class, CassandraDao.class, Customer360GraphDao.class})
public class SampleGraphDaoTestIt extends AbstractDseTest {
    
    protected String getContactPointAdress()         { return "localhost";             }
    protected int    getContactPointPort()           { return 9042;                    }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.ONE; }
    
    @Autowired
    protected CassandraDao cassandraDao;
    
    @Autowired
    protected GraphDao graphDao;
    
    @Autowired
    protected Customer360GraphDao c360Dao;
    
    @Test
    public void listGraphs() throws Exception {
        Assertions.assertTrue(graphDao.listGraphNames().contains("gsk_c360"));
    }
  
    @Test
    public void loadGraph() {
        graphDao.loadGraph("gsk_c360");
    }
    
    @Test
    public void getEdgesofCluster() {
        System.out.println(graphDao.getEdgeNamesForVertex("gsk_c360", "customer"));
    }
    
    @Test
    public void loadCluster() {
        //graphDao.loadClusterByClusterId("gsk_c360", "c5f5ff596973bea940dfaca11e23d6c1265eae1e");
    }
    
    @Test
    public void listClusterIds() {
        System.out.println(c360Dao.listClusterIds("gsk_c360"));
    }
    
    @Test
    public void createGraph() {
        c360Dao.createGraphC360();
    }
    
    @Test
    public void testLoadGraph() {
        File f = new File("src/test/resources/golden.csv");
        Assert.assertTrue(f.exists());
        c360Dao.loadClustersAndCustomers(f, ',');
    }
}
