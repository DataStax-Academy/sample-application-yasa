package com.datastax.graph.test.it;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.datastax.driver.dse.DseSession;
import com.datastax.yasa.dse.conf.DseConfiguration;

@SpringBootTest
@ContextConfiguration(classes={DseConfiguration.class})
@RunWith(JUnitPlatform.class)
@ExtendWith(SpringExtension.class)
@TestPropertySource(locations="/graphui-config.properties")
public class SampleDseSpringBoot2Junit5Test {

    @Value("${dse.cassandra.keyspace}")
    private String keySpace;
    
    @Autowired
    private DseSession dseSession;
    
    @Test
    @DisplayName("Test connectivity to DSE Server")
    public void doesitConnectToDse() {
        Assertions.assertFalse(dseSession.isClosed());
        Assertions.assertEquals(keySpace, dseSession.getLoggedKeyspace());
    }
    
}

