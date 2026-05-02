package io.freesidenomad.sqlmq.bench;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.freesidenomad.sqlmq.bench.BenchHarness.RunResult;
import io.freesidenomad.sqlmq.bench.BenchProfiles.NamedProfile;
import io.freesidenomad.sqlmq.bench.ConcurrencyHarness.Profile;
import io.freesidenomad.sqlmq.client.SqlmqClient;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Standalone command-line performance harness for sqlmq.
 *
 * Same shape as Gatling / JMeter: a self-contained executable, parameterized via
 * CLI flags, producing reports on its own (no JUnit / surefire involvement).
 *
 * The companion JUnit {@code BakeOffRunner} is kept in test scope for short
 * smoke-style runs, but production performance work should drive this CLI in a
 * caller-sized JVM (e.g. {@code java -Xmx4g -jar harness/target/sqlmq-bench-cli.jar ...}).
 */
@Command(
    name = "sqlmq-bench",
    mixinStandardHelpOptions = true,
    version = "sqlmq-bench 0.1.0-SNAPSHOT",
    description = {
        "Standalone performance harness for sqlmq.",
        "",
        "Runs a matrix of (profile x storage x runs), records throughput and",
        "p50/p95/p99 latency, and writes bench-summary.md / bench-results.json /",
        "bench-results.csv to --output-dir.",
        "",
        "Recommended JVM sizing: java -Xmx4g -jar sqlmq-bench-cli.jar ..."
    }
)
public final class SqlmqBenchCli implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(SqlmqBenchCli.class);

    // -------- Mode (container vs external JDBC) --------

    static final class ConnectionMode {
        @Option(names = "--container",
                description = "Spin up a Testcontainers SQL Server 2022 Linux image, apply Flyway " +
                              "migrations from sql/migrations/, run, drop. Requires Docker. " +
                              "(Default if neither --container nor --jdbc-url is given.)")
        boolean container;

        @ArgGroup(exclusive = false, multiplicity = "0..1")
        ExternalDb external;
    }

    static final class ExternalDb {
        @Option(names = "--jdbc-url", required = true,
                description = "JDBC URL of an existing pre-migrated SQL Server. The caller is " +
                              "responsible for having sqlmq.create_queue etc. installed.")
        String jdbcUrl;

        @Option(names = "--jdbc-user", required = true,
                description = "JDBC username (required with --jdbc-url).")
        String jdbcUser;

        @Option(names = "--jdbc-password", required = true,
                description = "JDBC password (required with --jdbc-url).")
        String jdbcPassword;
    }

    @ArgGroup(exclusive = true, multiplicity = "0..1")
    ConnectionMode connection;

    // -------- Workload (built-in profiles vs custom) --------

    static final class WorkloadMode {
        @Option(names = "--profile", paramLabel = "NAME",
                description = "Built-in profile (repeatable). Choices: ${COMPLETION-CANDIDATES}. " +
                              "Default: medium. Use 'custom' with --producers/--consumers/" +
                              "--messages-per-producer to override.",
                completionCandidates = ProfileChoices.class)
        List<String> profiles;

        @ArgGroup(exclusive = false, multiplicity = "0..1")
        CustomWorkload custom;
    }

    static final class CustomWorkload {
        @Option(names = "--producers", required = true,
                description = "Custom workload: number of producer threads.")
        int producers;

        @Option(names = "--consumers", required = true,
                description = "Custom workload: number of consumer threads.")
        int consumers;

        @Option(names = "--messages-per-producer", required = true, paramLabel = "K",
                description = "Custom workload: messages each producer sends.")
        int messagesPerProducer;

        @Option(names = "--preload", paramLabel = "N", defaultValue = "0",
                description = "Pre-load N messages BEFORE consumers start (burst pattern). " +
                              "Most meaningful with --producers 0. Default: 0.")
        int preload;
    }

    @ArgGroup(exclusive = true, multiplicity = "0..1")
    WorkloadMode workload;

    @Option(names = "--storage", paramLabel = "VARIANT",
            description = "Storage variant(s) to test (repeatable). Choices: ondisk, inmemory, both. Default: both.")
    List<String> storage;

    @Option(names = "--batch-size", defaultValue = "10",
            description = "Consumer read max_count. Default: ${DEFAULT-VALUE}.")
    int batchSize;

    @Option(names = "--vt-seconds", defaultValue = "30",
            description = "Visibility timeout per read. Default: ${DEFAULT-VALUE}.")
    int vtSeconds;

    @Option(names = "--runs", defaultValue = "5",
            description = "Number of runs per (profile, storage). The reported median uses these. Default: ${DEFAULT-VALUE}.")
    int runs;

    @Option(names = "--quiet-period-ms", defaultValue = "1000",
            description = "Drain detection threshold. Default: ${DEFAULT-VALUE}.")
    long quietPeriodMs;

    // -------- Output --------

    @Option(names = "--output-dir", paramLabel = "DIR", defaultValue = "./bench-results",
            description = "Where to write bench-summary.md / bench-results.json / .csv. " +
                          "A timestamped subdirectory is created inside. Default: ${DEFAULT-VALUE}.")
    String outputDir;

    @Option(names = "--report", paramLabel = "FORMAT", defaultValue = "all",
            description = "Output format(s). Choices: md, json, csv, all. Default: ${DEFAULT-VALUE}.")
    String report;

    @Option(names = "--no-progress", description = "Suppress per-run progress lines on stderr.")
    boolean noProgress;

    public static class ProfileChoices extends ArrayList<String> {
        public ProfileChoices() {
            super(BenchProfiles.builtinNames());
            add("custom");
        }
    }

    @Override
    public Integer call() throws Exception {
        // Resolve which profiles to run.
        var resolvedProfiles = resolveProfiles();
        var resolvedStorage = resolveStorage();
        var formats = resolveFormats();

        // Resolve connection mode and possibly start a container.
        BenchContainer container = null;
        HikariDataSource ds = null;
        String containerImage = null;
        try {
            String jdbcUrl, user, pwd;
            if (connection != null && connection.external != null) {
                jdbcUrl = connection.external.jdbcUrl;
                user = connection.external.jdbcUser;
                pwd = connection.external.jdbcPassword;
                log.info("Using external JDBC URL: {}", jdbcUrl);
            } else {
                // --container is the default when no mode flag is given.
                container = new BenchContainer();
                var mssql = container.start();
                containerImage = container.image();
                log.info("Started Testcontainers SQL Server: {}", containerImage);

                // Create a fresh database in the container, migrate it.
                var dbName = "sqlmq_bench_" + UUID.randomUUID().toString().replace("-", "");
                try (Connection adminConn = java.sql.DriverManager.getConnection(
                        mssql.getJdbcUrl(), mssql.getUsername(), mssql.getPassword());
                     Statement st = adminConn.createStatement()) {
                    st.execute("CREATE DATABASE [" + dbName + "]");
                }
                jdbcUrl = mssql.getJdbcUrl() + ";databaseName=" + dbName + ";encrypt=false";
                user = mssql.getUsername();
                pwd = mssql.getPassword();

                var migrationsLocation = resolveMigrationsLocation();
                log.info("Applying Flyway migrations from {}", migrationsLocation);
                Flyway.configure()
                      .dataSource(jdbcUrl, user, pwd)
                      .locations("filesystem:" + migrationsLocation)
                      .load()
                      .migrate();
            }

            // Pool sized to cover the largest profile in the matrix.
            int maxConcurrency = resolvedProfiles.stream()
                .mapToInt(np -> np.profile().producers() + np.profile().consumers())
                .max().orElse(64);
            // Leave headroom; cap to keep DB conn count bounded.
            int poolSize = Math.min(Math.max(64, maxConcurrency + 16), 256);

            var hcfg = new HikariConfig();
            hcfg.setJdbcUrl(jdbcUrl);
            hcfg.setUsername(user);
            hcfg.setPassword(pwd);
            hcfg.setMaximumPoolSize(poolSize);
            hcfg.setConnectionTimeout(10_000);
            ds = new HikariDataSource(hcfg);
            var client = new SqlmqClient(ds);

            // Prepare output dir.
            var ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
            var outDir = Paths.get(outputDir).toAbsolutePath().resolve(ts);
            Files.createDirectories(outDir);
            log.info("Output dir: {}", outDir);

            var startedAt = Instant.now();
            var reporter = new BenchReporter(containerImage, startedAt);

            var stampFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
            for (var named : resolvedProfiles) {
                for (var stor : resolvedStorage) {
                    if (!noProgress) {
                        System.err.printf("=== %s/%s (%d runs) ===%n", named.name(), stor, runs);
                    }
                    for (int i = 1; i <= runs; i++) {
                        var sample = BenchHarness.runOnce(client, named.name(), named.profile(),
                                                          named.preloadCount(), stor, i);
                        reporter.add(sample);
                        if (!noProgress) {
                            var stamp = LocalDateTime.now(ZoneId.systemDefault()).format(stampFmt);
                            System.err.printf(Locale.ROOT,
                                "[%s] %s/%s run %d/%d: %.0f msgs/sec p50=%.1fms p95=%.1fms p99=%.1fms delivered=%d elapsed=%.1fs%n",
                                stamp, named.name(), stor, i, runs,
                                sample.msgsPerSec(), sample.p50Ms(), sample.p95Ms(), sample.p99Ms(),
                                sample.delivered(), sample.elapsedSeconds());
                        }
                        // Incremental flush after every run so a crash mid-matrix still
                        // leaves recoverable data on disk.
                        reporter.writeAll(outDir, formats, Instant.now());
                    }
                }
            }
            var completedAt = Instant.now();
            reporter.writeAll(outDir, formats, completedAt);

            // Final summary to stdout.
            System.out.println(reporter.markdown(completedAt));
            System.out.println("Results written to: " + outDir);
            return 0;
        } finally {
            if (ds != null) {
                try { ds.close(); } catch (Throwable ignored) {}
            }
            if (container != null) container.stop();
        }
    }

    private List<NamedProfile> resolveProfiles() {
        // Custom workload?
        if (workload != null && workload.custom != null) {
            // Reject mixing 'custom' overrides with built-in profile names:
            // the user already picked a workload via the CustomWorkload arg group.
            if (workload.profiles != null) {
                var nonCustom = workload.profiles.stream().filter(n -> !"custom".equalsIgnoreCase(n)).toList();
                if (!nonCustom.isEmpty()) {
                    throw new CommandLine.ParameterException(new CommandLine(this),
                        "Cannot combine --producers/--consumers/--messages-per-producer with built-in --profile " +
                        nonCustom + ". Drop the built-in profile name(s) (or use --profile custom).");
                }
            }
            // --preload makes most sense with --producers 0; warn (not reject) otherwise.
            if (workload.custom.preload > 0 && workload.custom.producers > 0) {
                System.err.println("Warning: --preload combined with --producers > 0 mixes burst and steady-state " +
                                   "patterns; latency numbers will be hard to interpret.");
            }
            var p = new Profile(workload.custom.producers, workload.custom.consumers,
                                workload.custom.messagesPerProducer,
                                batchSize, vtSeconds, quietPeriodMs);
            return List.of(new NamedProfile("custom", p, workload.custom.preload));
        }

        // Built-in profile name(s).
        var names = (workload != null && workload.profiles != null && !workload.profiles.isEmpty())
            ? new LinkedHashSet<>(workload.profiles)
            : new LinkedHashSet<>(List.of("medium"));

        var out = new ArrayList<NamedProfile>();
        for (var n : names) {
            if ("custom".equalsIgnoreCase(n)) {
                throw new CommandLine.ParameterException(new CommandLine(this),
                    "--profile custom requires --producers, --consumers, --messages-per-producer.");
            }
            var np = BenchProfiles.byName(n);
            if (np == null) {
                throw new CommandLine.ParameterException(new CommandLine(this),
                    "Unknown profile '" + n + "'. Choices: " + BenchProfiles.builtinNames() + " or 'custom'.");
            }
            // Apply CLI overrides for batch/vt/quiet to built-in profile's Profile record.
            var base = np.profile();
            var adjusted = new Profile(base.producers(), base.consumers(), base.messagesPerProducer(),
                                       batchSize != 10 ? batchSize : base.batchSize(),
                                       vtSeconds != 30 ? vtSeconds : base.vtSeconds(),
                                       quietPeriodMs != 1000 ? quietPeriodMs : base.quietPeriodMs());
            out.add(new NamedProfile(np.name(), adjusted, np.preloadCount()));
        }
        return out;
    }

    private List<String> resolveStorage() {
        if (storage == null || storage.isEmpty()) {
            return List.of("ondisk", "inmemory");
        }
        var out = new LinkedHashSet<String>();
        for (var s : storage) {
            switch (s.toLowerCase(Locale.ROOT)) {
                case "ondisk"   -> out.add("ondisk");
                case "inmemory" -> out.add("inmemory");
                case "both"     -> { out.add("ondisk"); out.add("inmemory"); }
                default -> throw new CommandLine.ParameterException(new CommandLine(this),
                    "Unknown --storage value '" + s + "'. Choices: ondisk, inmemory, both.");
            }
        }
        return new ArrayList<>(out);
    }

    private Set<String> resolveFormats() {
        var f = report.toLowerCase(Locale.ROOT);
        if ("all".equals(f)) return Set.of("md", "json", "csv");
        var out = new LinkedHashSet<String>();
        for (var part : f.split(",")) {
            part = part.trim();
            if (part.isEmpty()) continue;
            if (!Set.of("md", "json", "csv").contains(part)) {
                throw new CommandLine.ParameterException(new CommandLine(this),
                    "Unknown --report value '" + part + "'. Choices: md, json, csv, all.");
            }
            out.add(part);
        }
        if (out.isEmpty()) out.add("md");
        return out;
    }

    /**
     * Find the sql/migrations/ directory. The CLI may be launched from arbitrary cwds;
     * the layout assumption is that the user runs from the repo root, but we also
     * tolerate being run from inside harness/.
     */
    private static String resolveMigrationsLocation() {
        var candidates = List.of(
            Paths.get("sql", "migrations"),
            Paths.get("..", "sql", "migrations"),
            Paths.get("../..", "sql", "migrations")
        );
        for (var c : candidates) {
            if (Files.isDirectory(c)) return c.toAbsolutePath().normalize().toString();
        }
        // Fall back to the literal default — Flyway will surface a clear error.
        return Paths.get("sql", "migrations").toAbsolutePath().normalize().toString();
    }

    public static void main(String[] args) {
        // Quiet down testcontainers/SLF4J unless the caller wants chatty logs.
        if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null) {
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        }
        if (System.getProperty("org.slf4j.simpleLogger.log.io.freesidenomad.sqlmq") == null) {
            System.setProperty("org.slf4j.simpleLogger.log.io.freesidenomad.sqlmq", "info");
        }
        int exitCode = new CommandLine(new SqlmqBenchCli()).execute(args);
        System.exit(exitCode);
    }
}
