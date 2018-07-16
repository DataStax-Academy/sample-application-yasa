package com.datastax.yasa.ui.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.thymeleaf.context.WebContext;

import com.datastax.yasa.dse.dao.GraphDao;
import com.datastax.yasa.ui.webbean.GraphWebBean;

/**
 * Working with GRAPH
 * @author Cedrick LUNVEN (@clunven)
 */
@Controller
@RequestMapping("/graph")
public class GraphController extends AbstractController {

    /** View name. */
    private static final String GRAPH_VIEW      = "graph";
    
    /** Params. */
    private static final String PARAM_GRAPHNAME = "graphname";
    
    /** Graph. */
    @Autowired
    private GraphDao graphDao;
    
    /** {@inheritDoc} */
    @Override
    public String getSuccessView() {
        return GRAPH_VIEW;
    }
    
    /** {@inheritDoc} */
    @Override
    public void get(HttpServletRequest req, HttpServletResponse res, WebContext ctx) throws Exception {
        GraphWebBean gwb = (GraphWebBean) ctx.getVariable("gbean");
        if (gwb == null) {
            gwb = new GraphWebBean();
            gwb.getUserGraphs().addAll(graphDao.listGraphNames());
            LOGGER.info("GraphList loaded {}", gwb.getUserGraphs());
        }
        
        String graphName = req.getParameter(PARAM_GRAPHNAME);
        if (null != graphName && graphName.length() > 0) {
            gwb.setGraphName(graphName);
        }
        ctx.setVariable("gbean", gwb);
    }

    /** {@inheritDoc} */
    @Override
    public void processPost(HttpServletRequest req, HttpServletResponse res, WebContext ctx) throws Exception {
        GraphWebBean gwb = new GraphWebBean();
        gwb.getUserGraphs().addAll(graphDao.listGraphNames());
        ctx.setVariable("gbean", gwb);
    }
  
}
