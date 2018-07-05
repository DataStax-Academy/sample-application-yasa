package com.datastax.yasa.dse.conf;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.dse.DseCluster.Builder;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.auth.DsePlainTextAuthProvider;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.driver.dse.graph.GraphProtocol;
import com.datastax.driver.mapping.DefaultPropertyMapper;
import com.datastax.driver.mapping.MappingConfiguration;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.PropertyMapper;
import com.datastax.driver.mapping.PropertyTransienceStrategy;
import com.datastax.yasa.dse.utils.BlobToStringCodec;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;

/**
 * Connectivity to DSE (cassandra, graph, search, analytics).
 *
 * @author DataStax evangelist team.
 */
@Configuration
public class DseConfiguration {

	/** Internal logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(DseConfiguration.class);
    
    // -- Cassandra --
    
    @Value("${dse.cassandra.username}")
    public Optional < String > dseUsername;
   
    @Value("${dse.cassandra.password}")
    public Optional < String > dsePassword;
    
    @Value("#{'${dse.cassandra.contactPoints}'.split(',')}")
    public List < String > contactPoints;
    
    @Value("${dse.cassandra.port: 9042}")
    public int port;
    
    @Value("${dse.cassandra.keyspace: system}")
    public String keyspace;
    
    // -- Graph --
    
    @Value("${dse.graph.enable}")
    public boolean graphEnabled;
    
    @Value("${dse.graph.timeout: 30000}")
    public Integer graphTimeout;
   
    // -- Retry mechanism --
    
    @Value("${dse.retry.enabled: false}")
    private boolean enableRetries;
    
    @Value("${dse.retry.maxNumberOfTries: 10}")
    private int maxNumberOfTries  = 10;
    
    @Value("${dse.retry.delayBetweenTries: 2}")
    private int delayBetweenTries = 2;
    
    @Bean
    public DseSession dseSession() {
        long top = System.currentTimeMillis();
        LOGGER.info("Initializing connection to DSE Cluster");
        Builder clusterConfig = new Builder();

        // CASSANDRA
        LOGGER.info(" + DSE Contact Points : {}" , contactPoints);
        contactPoints.stream().forEach(clusterConfig::addContactPoint);
        LOGGER.info(" + DSE DB Port : {}", port);
        clusterConfig.withPort(port);
        if (dseUsername.isPresent() && dsePassword.isPresent() && dseUsername.get().length() > 0) {
            AuthProvider cassandraAuthProvider = new DsePlainTextAuthProvider(dseUsername.get(), dsePassword.get());
            clusterConfig.withAuthProvider(cassandraAuthProvider);
            LOGGER.info(" + With username  : {}", dseUsername.get());
        }

        // GRAPH
        LOGGER.info(" + DSE Graph : enabled '{}' timeout '{}'", graphEnabled, graphTimeout);
        if (graphEnabled) {
            GraphOptions graphOption = new GraphOptions();
            graphOption.setReadTimeoutMillis(graphTimeout);
            //graphOption.setGraphName("graohName");
            //graphOption.setGraphWriteConsistencyLevel(ConsistencyLevel.QUORUM);
            //graphOption.setGraphReadConsistencyLevel(ConsistencyLevel.QUORUM);
            graphOption.setGraphSubProtocol(GraphProtocol.GRAPHSON_2_0);
            clusterConfig.withGraphOptions(graphOption);
        }
         
        // OPTIONS
        QueryOptions options = new QueryOptions();
        options.setConsistencyLevel(ConsistencyLevel.QUORUM);
        options.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        clusterConfig.withQueryOptions(options);
        clusterConfig.withoutJMXReporting();
        clusterConfig.withoutMetrics();
        clusterConfig.withReconnectionPolicy(new ConstantReconnectionPolicy(2000));
        clusterConfig.getConfiguration().getCodecRegistry().register(new BlobToStringCodec());
         
        // Callable for retry
        final AtomicInteger atomicCount = new AtomicInteger(1);
        Callable<DseSession> connectionToDse = () -> {
            return clusterConfig.build().connect(keyspace);
        };
         
        // Retry Settings
        RetryConfig config = new RetryConfigBuilder()
                 .retryOnAnyException()
                 .withMaxNumberOfTries(maxNumberOfTries)
                 .withDelayBetweenTries(delayBetweenTries, ChronoUnit.SECONDS)
                 .withFixedBackoff()
                 .build();
         
        // Executing connecton with retries
        return new CallExecutor<DseSession>(config)
                 .afterFailedTry(s -> { 
                     LOGGER.info(" + Attempt #{}/{} failed with ({})  trying in {} seconds ", atomicCount.getAndIncrement(),
                             maxNumberOfTries, s.getLastExceptionThatCausedRetry().getMessage(), delayBetweenTries);
                     })
                 .onFailure(s -> {
                     LOGGER.error(" >< Cannot connect to DSE after {} attempts, exiting.", maxNumberOfTries);
                     System.err.println("Cannot connect to DSE after " + maxNumberOfTries + " attempts, exiting.");
                     System.exit(500);
                  })
                 .onSuccess(s -> {   
                     long timeElapsed = System.currentTimeMillis() - top;
                     LOGGER.info(" + Connection established to DSE Cluster \\_0_/ in {} millis.", timeElapsed);})
                 .execute(connectionToDse).getResult();
    }
    
    /**
     * Use to create mapper and perform ORM on top of Cassandra tables.
     * 
     * @param session
     *      current dse session.
     * @return
     *      mapper
     */
    @Bean
    public MappingManager mappingManager(DseSession session) {
        // Do not map all fields, only the annotated ones with @Column or @Fields
        PropertyMapper propertyMapper = new DefaultPropertyMapper()
                .setPropertyTransienceStrategy(PropertyTransienceStrategy.OPT_IN);
        // Build configuration from mapping
        MappingConfiguration configuration = MappingConfiguration.builder()
                .withPropertyMapper(propertyMapper)
                .build();
        // Sample Manager with advance configuration
        return new MappingManager(session, configuration);
    }
    
}
