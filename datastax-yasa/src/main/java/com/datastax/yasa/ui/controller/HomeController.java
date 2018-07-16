package com.datastax.yasa.ui.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.thymeleaf.context.WebContext;

import com.datastax.driver.core.Host;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.yasa.ui.webbean.DseNodeWebBean;
import com.datastax.yasa.ui.webbean.HomeWebBean;

@Controller
@RequestMapping(value="/")
public class HomeController extends AbstractController {
    
    /** Vie name. */
    private static final String HOME_VIEW = "home";
    
    @Autowired
    private DseSession dseSession;

    /** {@inheritDoc} */
    @Override
    public String getSuccessView() {
        return HOME_VIEW;
    }
    
    /** {@inheritDoc} */
    @Override
    public void get(HttpServletRequest req, HttpServletResponse res, WebContext ctx) 
    throws Exception {
        ctx.setVariable("homebean", mapCluster(dseSession.getCluster()));
    }
    
    /** {@inheritDoc} */
    @Override
    public void processPost(HttpServletRequest req, HttpServletResponse res, WebContext ctx) 
    throws Exception {
    }
    
    /**
     * Map to web bean.
     *
     * @param cluster
     *          current datastax Cluster instance
     * @return
     *      werb bean
     */
    private HomeWebBean mapCluster(final DseCluster cluster) {
        HomeWebBean home = new HomeWebBean();
        home.setClusterName(cluster.getMetadata().getClusterName());
        home.setDseDriverVersion(DseCluster.getDseDriverVersion());
        for (Host host : cluster.getMetadata().getAllHosts()) {
            DseNodeWebBean dseNode = new DseNodeWebBean();
            dseNode.setHostname(host.getAddress().getHostName());
            dseNode.setHostAdress(host.getAddress().getHostAddress());
            dseNode.setDseVersion(host.getDseVersion().getMajor() + "." +
                    host.getDseVersion().getMinor() + "." +
                    host.getDseVersion().getPatch());
            dseNode.setState(host.getState());
            dseNode.getWorkloads().addAll(host.getDseWorkloads());
            home.getDseNodeList().add(dseNode);
        }
        
        // Po licies
        home.setLoadBalancingPolicy(cluster.getConfiguration().getPolicies().getLoadBalancingPolicy().getClass().getSimpleName());
        home.setReconnectionPolicy(cluster.getConfiguration().getPolicies().getReconnectionPolicy().getClass().getSimpleName());
        home.setRetryPolicy(cluster.getConfiguration().getPolicies().getRetryPolicy().getClass().getSimpleName());
        
        // Query Options
        home.setQueryRefreshNodeInterval(cluster.getConfiguration().getQueryOptions().getRefreshNodeIntervalMillis());
        home.setQueryRefreshNodeMaxPendingRequest(cluster.getConfiguration().getQueryOptions().getMaxPendingRefreshNodeRequests());
        home.setQueryRefreshNodeListInterval(cluster.getConfiguration().getQueryOptions().getRefreshNodeListIntervalMillis());
        home.setQueryRefreshNodeListMaxPendingRequest(cluster.getConfiguration().getQueryOptions().getMaxPendingRefreshNodeListRequests());
        home.setQueryRefreshSchemaInterval(cluster.getConfiguration().getQueryOptions().getRefreshSchemaIntervalMillis());
        home.setQueryRefreshSchemaPendingRequest(cluster.getConfiguration().getQueryOptions().getMaxPendingRefreshSchemaRequests());
        home.setQueryConsistencyLevel(cluster.getConfiguration().getQueryOptions().getConsistencyLevel().toString());
        home.setQueryFetchSize(cluster.getConfiguration().getQueryOptions().getFetchSize());
        home.setQueryDefaultIdempotence(cluster.getConfiguration().getQueryOptions().getDefaultIdempotence());
        return home;
    }
  
   
}
