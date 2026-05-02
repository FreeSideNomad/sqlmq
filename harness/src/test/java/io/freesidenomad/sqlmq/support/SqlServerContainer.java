package io.freesidenomad.sqlmq.support;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One MSSQLServerContainer per JVM. Started lazily on first access.
 * Tests get their own database via {@link DatabasePerTest}.
 */
public final class SqlServerContainer {
    private static final DockerImageName IMAGE =
        DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest");

    private static volatile MSSQLServerContainer<?> instance;

    private SqlServerContainer() {}

    public static MSSQLServerContainer<?> get() {
        var local = instance;
        if (local == null) {
            synchronized (SqlServerContainer.class) {
                local = instance;
                if (local == null) {
                    local = new MSSQLServerContainer<>(IMAGE).acceptLicense();
                    local.start();
                    instance = local;
                }
            }
        }
        return local;
    }

    public static String adminJdbcUrl() {
        return get().getJdbcUrl();
    }

    public static String adminUser() {
        return get().getUsername();
    }

    public static String adminPassword() {
        return get().getPassword();
    }
}
