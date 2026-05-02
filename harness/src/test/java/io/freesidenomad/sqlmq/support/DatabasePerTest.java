package io.freesidenomad.sqlmq.support;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

/**
 * Provisions a fresh database per test class, applies all Flyway migrations,
 * exposes a HikariDataSource scoped to that database, drops it after the class.
 *
 * Use via: @ExtendWith(DatabasePerTest.class) on the test class.
 * Retrieve the DataSource via DatabasePerTest.dataSource(ctx).
 */
public final class DatabasePerTest implements BeforeAllCallback, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger(DatabasePerTest.class);
    private static final Namespace NS = Namespace.create(DatabasePerTest.class);

    @Override
    public void beforeAll(ExtensionContext ctx) throws Exception {
        var container = SqlServerContainer.get();
        var dbName = "sqlmq_test_" + UUID.randomUUID().toString().replace("-", "");

        try (Connection adminConn = java.sql.DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement st = adminConn.createStatement()) {
            st.execute("CREATE DATABASE [" + dbName + "]");
        }

        // From this point on, if anything throws we must drop the database before
        // the exception propagates — otherwise the DB is leaked inside the long-lived
        // container (JUnit does not call afterAll if beforeAll throws).
        HikariDataSource ds = null;
        try {
            var jdbcUrl = container.getJdbcUrl() + ";databaseName=" + dbName + ";encrypt=false";
            var hikariCfg = new HikariConfig();
            hikariCfg.setJdbcUrl(jdbcUrl);
            hikariCfg.setUsername(container.getUsername());
            hikariCfg.setPassword(container.getPassword());
            hikariCfg.setMaximumPoolSize(64);
            hikariCfg.setConnectionTimeout(5000);
            ds = new HikariDataSource(hikariCfg);

            Flyway.configure()
                  .dataSource(ds)
                  .locations("filesystem:../sql/migrations")
                  .load()
                  .migrate();

            ctx.getStore(NS).put("dbName", dbName);
            ctx.getStore(NS).put("dataSource", ds);
        } catch (Throwable t) {
            if (ds != null) {
                try {
                    ds.close();
                } catch (Throwable closeEx) {
                    t.addSuppressed(closeEx);
                }
            }
            try {
                dropDatabase(container, dbName);
            } catch (Throwable dropEx) {
                t.addSuppressed(dropEx);
            }
            throw t;
        }
    }

    @Override
    public void afterAll(ExtensionContext ctx) throws Exception {
        var ds = (HikariDataSource) ctx.getStore(NS).get("dataSource");
        if (ds != null) {
            try {
                ds.close();
            } catch (Throwable t) {
                log.warn("Failed to close HikariDataSource", t);
            }
        }

        var container = SqlServerContainer.get();
        var dbName = (String) ctx.getStore(NS).get("dbName");
        if (dbName != null) {
            try {
                dropDatabase(container, dbName);
            } catch (Throwable t) {
                log.warn("Failed to drop test database [{}]; it may be leaked inside the test container", dbName, t);
            }
        }
    }

    public static DataSource dataSource(ExtensionContext ctx) {
        return (DataSource) ctx.getStore(NS).get("dataSource");
    }

    private static void dropDatabase(org.testcontainers.containers.MSSQLServerContainer<?> container, String dbName) throws Exception {
        try (Connection adminConn = java.sql.DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement st = adminConn.createStatement()) {
            // Run SINGLE_USER and DROP as separate statements — if DROP fails we still
            // want the failure surfaced rather than silently leaving the DB locked.
            st.execute("ALTER DATABASE [" + dbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
            st.execute("DROP DATABASE [" + dbName + "]");
        }
    }
}
