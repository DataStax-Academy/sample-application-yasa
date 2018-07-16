package com.datastax.graph.test;

import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.yasa.dse.conf.DseConfiguration;
import com.datastax.yasa.dse.dao.CassandraDao;
import com.datastax.yasa.dse.dao.GraphDao;

@ContextConfiguration(classes={DseConfiguration.class, GraphDao.class, CassandraDao.class})
public class SampleCassandraDaoTestIt extends AbstractDseTest {
    
    @Autowired
    protected CassandraDao cassandraDao;
    
    protected String getContactPointAdress()         { return "localhost";             }
    protected int    getContactPointPort()           { return 9042;                    }
    protected ConsistencyLevel getConsistencyLevel() { return ConsistencyLevel.ONE;    }
    
    @Test
    public void listKeyspaces() throws Exception {
        System.out.println(cassandraDao.listCassandraKeyspacesNames());
    }
    
    @Test
    public void listTables() throws Exception {
        System.out.println(cassandraDao.listTablesNamesByKeySpace("evangelists").collect(Collectors.toList()));
    }
    
    @Test
    public void listColumns() throws Exception {
        System.out.println(cassandraDao.listColumns("evangelists", "conferences").collect(Collectors.toList()));
    }
    
    @Test
    public void listColumnsFull() throws Exception {
        System.out.println(cassandraDao.listColumnsFull("evangelists", "conferences"));
    }
      
    
        
}
