package com.datastax.yasa.customer360;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.yasa.ui.model.VizJsGraph;

@RestController
@RequestMapping("/api/v1/c360")
public class Customer360GraphResource {

    /** Internal logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(Customer360GraphResource.class);
    
    @Autowired
    protected Customer360GraphDao dao;
    
    @RequestMapping(value = "/clusters", method = GET, produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Map < String, String > > listClusters(@PathVariable(value = "graphName") String graphName) {
        LOGGER.info("List cluster ids in graph {}", graphName);
        return new ResponseEntity<Map < String, String > >(dao.listClusterIds(graphName), HttpStatus.ACCEPTED);
    }

    @RequestMapping(value = "/{graphName}/clusters/{clusterid}", method = GET, produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<VizJsGraph> loadclusterById(@PathVariable(value = "graphName") String graphName,
            @PathVariable(value = "clusterid") String clusterid) {
        LOGGER.info("Local graph for cluster {} on graph {}", clusterid, graphName);
        return new ResponseEntity<VizJsGraph>(dao.loadClusterByClusterId(graphName, clusterid), HttpStatus.OK);
    }
    
}
