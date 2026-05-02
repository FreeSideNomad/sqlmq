package io.freesidenomad.sqlmq.bench;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Standalone Testcontainers wrapper used by the bench CLI when invoked with --container.
 *
 * Intentionally separate from the test-scope {@code SqlServerContainer}: this is a
 * runtime concern (the CLI is not a test), and keeping it in main scope avoids
 * having the CLI depend on test classes.
 */
public final class BenchContainer {

    public static final String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2022-latest";

    private MSSQLServerContainer<?> container;

    public synchronized MSSQLServerContainer<?> start() {
        if (container == null) {
            container = new MSSQLServerContainer<>(DockerImageName.parse(IMAGE_NAME)).acceptLicense();
            container.start();
        }
        return container;
    }

    public synchronized void stop() {
        if (container != null) {
            try { container.stop(); } catch (Throwable ignored) {}
            container = null;
        }
    }

    public String image() { return IMAGE_NAME; }
}
