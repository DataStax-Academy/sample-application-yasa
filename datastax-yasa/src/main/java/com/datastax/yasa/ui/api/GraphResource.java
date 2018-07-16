package com.datastax.yasa.ui.api;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.yasa.dse.dao.GraphDao;
import com.datastax.yasa.ui.model.VizJsGraph;

@RestController
@RequestMapping("/api/v1/graphs")
public class GraphResource {

    /** Internal logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphResource.class);

    @Autowired
    protected GraphDao graphDao;

    @RequestMapping(value = "/", method = GET, produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Set<String>> loadGraphByName() {
        Set<String> grapList = graphDao.listGraphNames();
        LOGGER.info("Display Graph list " + grapList);
        return new ResponseEntity<Set<String>>(grapList, HttpStatus.OK);
    }
    
    @RequestMapping(value = "/{graphName}", method = POST,  produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<VizJsGraph> executeGremlinQuery(@PathVariable(value = "graphName") String graphName, @RequestBody String gremlinQuery) {
        return new ResponseEntity<VizJsGraph>(
                graphDao.executeGremlinQuery(graphName, gremlinQuery, true), HttpStatus.ACCEPTED);
    }
    
    @RequestMapping(value = "/{graphName}/{gremlinQuery}", method = GET,  produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<VizJsGraph> executeGremlinQueryGeg(@PathVariable(value = "graphName") String graphName, 
            @PathVariable(value = "gremlinQuery") String gremlinQuery) {
        return new ResponseEntity<VizJsGraph>(
                graphDao.executeGremlinQuery(graphName, gremlinQuery, true), HttpStatus.ACCEPTED);
    }
    
    

}
