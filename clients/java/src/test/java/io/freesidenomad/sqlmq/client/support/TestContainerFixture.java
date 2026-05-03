package io.freesidenomad.sqlmq.client.support;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;

/**
 * JUnit 5 extension that provisions a fresh database per test class against
 * a JVM-singleton SQL Server 2022 testcontainer, applies all sqlmq Flyway
 * migrations from {@code ../../sql/migrations/}, exposes a Hikari-backed
 * {@link DataSource} scoped to that database, and tears the database down
 * in {@code afterAll}.
 *
 * <p>Tests opt in via {@code @ExtendWith(TestContainerFixture.class)} on
 * the class. Test classes can request a {@link DataSource} or a
 * {@link com.microsoft.sqlserver.jdbc.SQLServerDataSource}-style
 * connection bag through method/constructor parameter injection or
 * pull it via {@link #dataSource(ExtensionContext)}.</p>
 */
public final class TestContainerFixture implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private static final DockerImageName IMAGE =
        DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest");

    private static final Namespace NS = Namespace.create(TestContainerFixture.class);

    /** Lazily-started, JVM-shared SQL Server container. */
    private static volatile MSSQLServerContainer<?> container;

    public static MSSQLServerContainer<?> container() {
        var local = container;
        if (local == null) {
            synchronized (TestContainerFixture.class) {
                local = container;
                if (local == null) {
                    local = new MSSQLServerContainer<>(IMAGE).acceptLicense();
                    local.start();
                    container = local;
                }
            }
        }
        return local;
    }

    @Override
    public void beforeAll(ExtensionContext ctx) throws Exception {
        var c = container();
        var dbName = "sqlmq_test_" + UUID.randomUUID().toString().replace("-", "");

        try (Connection adminConn = DriverManager.getConnection(
                c.getJdbcUrl(), c.getUsername(), c.getPassword());
             Statement st = adminConn.createStatement()) {
            st.execute("CREATE DATABASE [" + dbName + "]");
        }

        HikariDataSource ds = null;
        try {
            var jdbcUrl = c.getJdbcUrl() + ";databaseName=" + dbName + ";encrypt=false";
            var cfg = new HikariConfig();
            cfg.setJdbcUrl(jdbcUrl);
            cfg.setUsername(c.getUsername());
            cfg.setPassword(c.getPassword());
            cfg.setMaximumPoolSize(16);
            cfg.setConnectionTimeout(5_000);
            ds = new HikariDataSource(cfg);

            MigrationApplier.applyAll(ds);

            ctx.getStore(NS).put("dbName", dbName);
            ctx.getStore(NS).put("dataSource", ds);
            ctx.getStore(NS).put("jdbcUrl", jdbcUrl);
            ctx.getStore(NS).put("username", c.getUsername());
            ctx.getStore(NS).put("password", c.getPassword());
        } catch (Throwable t) {
            // beforeAll failure means no afterAll — don't leak the DB.
            if (ds != null) try { ds.close(); } catch (Throwable closeEx) { t.addSuppressed(closeEx); }
            try { dropDatabase(c, dbName); } catch (Throwable dropEx) { t.addSuppressed(dropEx); }
            throw t;
        }
    }

    @Override
    public void afterAll(ExtensionContext ctx) {
        var ds = (HikariDataSource) ctx.getStore(NS).get("dataSource");
        if (ds != null) try { ds.close(); } catch (Throwable ignored) {}
        var dbName = (String) ctx.getStore(NS).get("dbName");
        if (dbName != null) {
            try {
                dropDatabase(container(), dbName);
            } catch (Throwable t) {
                System.err.println("[TestContainerFixture] failed to drop " + dbName + ": " + t);
            }
        }
    }

    public static DataSource dataSource(ExtensionContext ctx) {
        return (DataSource) ctx.getStore(NS).get("dataSource");
    }

    public static String jdbcUrl(ExtensionContext ctx) {
        return (String) ctx.getStore(NS).get("jdbcUrl");
    }

    public static String username(ExtensionContext ctx) {
        return (String) ctx.getStore(NS).get("username");
    }

    public static String password(ExtensionContext ctx) {
        return (String) ctx.getStore(NS).get("password");
    }

    @Override
    public boolean supportsParameter(ParameterContext pc, ExtensionContext ec) {
        return pc.getParameter().getType() == DataSource.class
            || pc.getParameter().getType() == ExtensionContext.class;
    }

    @Override
    public Object resolveParameter(ParameterContext pc, ExtensionContext ec) throws ParameterResolutionException {
        Class<?> type = pc.getParameter().getType();
        if (type == DataSource.class) return dataSource(ec);
        if (type == ExtensionContext.class) return ec;
        throw new ParameterResolutionException("unsupported parameter: " + type);
    }

    private static void dropDatabase(MSSQLServerContainer<?> c, String dbName) throws Exception {
        try (Connection adminConn = DriverManager.getConnection(
                c.getJdbcUrl(), c.getUsername(), c.getPassword());
             Statement st = adminConn.createStatement()) {
            st.execute("ALTER DATABASE [" + dbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
            st.execute("DROP DATABASE [" + dbName + "]");
        }
    }
}
