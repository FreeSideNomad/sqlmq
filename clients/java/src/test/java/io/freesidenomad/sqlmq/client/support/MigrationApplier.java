package io.freesidenomad.sqlmq.client.support;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

/**
 * Apply all sqlmq Flyway migrations from the monorepo's
 * {@code sql/migrations/} directory to a target {@link DataSource}.
 *
 * <p>The location is computed relative to the {@code clients/java/}
 * working directory that Maven runs surefire from.</p>
 */
public final class MigrationApplier {

    private MigrationApplier() {}

    public static void applyAll(DataSource ds) {
        Flyway.configure()
            .dataSource(ds)
            .locations("filesystem:../../sql/migrations")
            .load()
            .migrate();
    }
}
