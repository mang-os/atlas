package com.atlas.outbox;

import java.util.function.Function;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Dedicated small pool: broker waits never consume the API/consumer pool. Not a second auto-configured DataSource bean. */
@Component
@ConditionalOnProperty(prefix = "atlas.kafka", name = "enabled", havingValue = "true")
public class PublisherTransactions implements AutoCloseable {
    private final HikariDataSource pool;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public PublisherTransactions(DataSource applicationDataSource) throws java.sql.SQLException {
        var source = applicationDataSource.unwrap(HikariDataSource.class);
        var config = new HikariConfig();
        config.setJdbcUrl(source.getJdbcUrl());
        config.setUsername(source.getUsername());
        config.setPassword(source.getPassword());
        var connectionProperties = new java.util.Properties();
        connectionProperties.putAll(source.getDataSourceProperties());
        config.setDataSourceProperties(connectionProperties);
        config.addDataSourceProperty("options", "-c statement_timeout=10000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=15000");
        config.setConnectionInitSql("SET TIME ZONE 'UTC'");
        config.setPoolName("atlas-outbox-pool");
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(2000);
        config.setValidationTimeout(1000);
        config.setInitializationFailTimeout(-1);
        pool = new HikariDataSource(config);
        jdbc = new JdbcTemplate(pool);
        transaction = new TransactionTemplate(new JdbcTransactionManager(pool));
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setTimeout(12);
    }

    public <T> T execute(Function<JdbcTemplate,T> work) { return transaction.execute(status -> work.apply(jdbc)); }
    @Override public void close() { pool.close(); }
}
