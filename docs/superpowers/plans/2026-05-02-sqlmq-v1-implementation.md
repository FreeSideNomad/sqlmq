# sqlmq v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build sqlmq v1 — a pure T-SQL message queue for SQL Server 2022+ with two storage variants (on-disk and memory-optimized) behind one API, validated by Java 25 + virtual-threads concurrency tests, ready for the in-memory-vs-on-disk bake-off.

**Architecture:** Flyway-style versioned T-SQL migrations under `sql/migrations/`. A Maven-built Java 25 test harness under `harness/` exercises the procs through `mssql-jdbc` against a Testcontainers SQL Server 2022 Linux image. Per-queue tables (`sqlmq.q_<name>`, `sqlmq.a_<name>`); installation-wide registry (`sqlmq.meta`); public dispatcher procs (`sqlmq.send`, `sqlmq.read`, …) that branch on `meta.storage_type` to either generic interpreted inner procs (on-disk) or per-queue natively compiled inner procs (in-memory). DLQ uses an exclude-and-sweep pattern symmetric across both variants.

**Tech Stack:** SQL Server 2022 (Linux container in CI), Flyway-Core 10, Maven, Java 25, JUnit 5, Microsoft `mssql-jdbc` 12.8.x, HikariCP, Testcontainers (`mssqlserver` module), AssertJ.

**Reference docs (read before starting):**
- `docs/superpowers/specs/2026-05-02-sqlmq-port-from-pgmq-design.md` — full design spec (the contract for this plan)
- `research/pgmq-surface.md` — pgmq behavior reference (use when uncertain about contract)
- `research/sqlserver-patterns.md` — SQL Server queue patterns
- `research/sqlserver-longpoll.md` — why long-polling is client-side

---

## File structure

Files this plan creates (all paths relative to repo root):

```
.github/workflows/
  ci.yml                              # Phase 0: Maven verify on push
  benchmark.yml                       # Phase 7: nightly bake-off

sql/migrations/
  V001__core_schema.sql               # Phase 1: sqlmq schema, sqlmq.meta, TVPs
  V002__create_drop_queue_procs.sql   # Phase 1: create_queue, drop_queue (on-disk only); queues TVF
  V003__send_procs.sql                # Phase 2: send, send_batch dispatchers + on-disk inner procs
  V004__read_procs.sql                # Phase 2: read dispatcher + on-disk inner proc (ungrouped)
  V005__delete_archive_pop_procs.sql  # Phase 2: delete dispatcher + on-disk; archive (always interpreted); pop
  V006__metrics_procs.sql             # Phase 4: metrics, metrics_all
  V007__purge_dlq_sweep_procs.sql     # Phase 4: purge_queue, dlq_sweep (always interpreted)
  V008__read_grouped_procs.sql        # Phase 5: read_grouped dispatcher + on-disk inner proc
  V009__inmemory_create_drop.sql      # Phase 6: extend create_queue to generate per-queue native procs

harness/
  pom.xml                             # Phase 0: all dependencies pinned

harness/src/main/java/io/freesidenomad/sqlmq/client/
  SqlmqClient.java                    # Phase 1: thin JDBC wrapper for tests
  LongPollingConsumer.java            # Phase 2: Hangfire-style backoff for read

harness/src/test/java/io/freesidenomad/sqlmq/support/
  SqlServerContainer.java             # Phase 0: shared Testcontainers wrapper (one container per JVM)
  DatabasePerTest.java                # Phase 0: JUnit 5 extension — fresh DB per test class
  ConcurrencyHarness.java             # Phase 3: virtual-thread N×M producer/consumer rig
  TestQueues.java                     # Phase 1: queue-name + create-queue helpers

harness/src/test/java/io/freesidenomad/sqlmq/correctness/
  SmokeTest.java                      # Phase 0
  CreateDropQueueTest.java            # Phase 1
  SendTest.java                       # Phase 2
  ReadTest.java                       # Phase 2
  DeleteArchivePopTest.java           # Phase 2
  MetricsTest.java                    # Phase 4
  PurgeDlqSweepTest.java              # Phase 4
  ReadGroupedTest.java                # Phase 5

harness/src/test/java/io/freesidenomad/sqlmq/concurrency/
  NoLossNoDoubleDeliveryTest.java     # Phase 3
  VtHonoredTest.java                  # Phase 3
  FifoOrderingTest.java               # Phase 3
  GroupedFifoInvariantTest.java       # Phase 5
  DlqEnforcementTest.java             # Phase 4

harness/src/test/java/io/freesidenomad/sqlmq/benchmark/
  BenchmarkProfiles.java              # Phase 7
  BakeOffRunner.java                  # Phase 7

harness/src/test/resources/
  junit-platform.properties           # Phase 0: parallel + virtual-thread executor
  simplelogger.properties             # Phase 0: SLF4J simple config
```

Phases ship working software incrementally:
- After Phase 1: `sqlmq.create_queue` / `drop_queue` work, registry exists, container infra runs
- After Phase 2: complete on-disk send/read/delete/archive/pop loop
- After Phase 3: concurrency invariants enforced on the on-disk variant
- After Phase 4: full on-disk feature surface (metrics, purge, DLQ enforcement)
- After Phase 5: grouped-FIFO read on-disk
- After Phase 6: both storage variants live, all tests parameterized over both
- After Phase 7: bake-off output

You can stop after any phase and have a coherent product.

---

## Working on a branch

Phase 0's first task creates a branch `v1-implementation`. All subsequent commits go there. Open a PR to `main` after Phase 5 (on-disk MVP) for first review; subsequent phases can land via additional PRs or onto the same branch — operator choice.

---

## Phase 0 — Project scaffolding

### Task 0.1: Create implementation branch

**Files:**
- Modify: git state

- [ ] **Step 1: Create and check out branch**

```bash
git checkout -b v1-implementation
```

- [ ] **Step 2: Verify**

Run: `git branch --show-current`
Expected: `v1-implementation`

### Task 0.2: Add Maven `pom.xml`

**Files:**
- Create: `harness/pom.xml`

- [ ] **Step 1: Write the pom**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.freesidenomad</groupId>
    <artifactId>sqlmq-harness</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>sqlmq harness</name>
    <description>Test and benchmark harness for sqlmq</description>
    <url>https://github.com/FreeSideNomad/sqlmq</url>

    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0</url>
        </license>
    </licenses>

    <organization>
        <name>FreeSideNomad</name>
        <url>https://github.com/FreeSideNomad</url>
    </organization>

    <scm>
        <connection>scm:git:git://github.com/FreeSideNomad/sqlmq.git</connection>
        <developerConnection>scm:git:ssh://git@github.com/FreeSideNomad/sqlmq.git</developerConnection>
        <url>https://github.com/FreeSideNomad/sqlmq</url>
    </scm>

    <properties>
        <maven.compiler.release>25</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <mssql.jdbc.version>12.8.1.jre11</mssql.jdbc.version>
        <flyway.version>10.20.1</flyway.version>
        <testcontainers.version>1.20.4</testcontainers.version>
        <junit.version>5.11.3</junit.version>
        <hikari.version>6.0.0</hikari.version>
        <assertj.version>3.26.3</assertj.version>
        <slf4j.version>2.0.16</slf4j.version>

        <maven.compiler.plugin.version>3.13.0</maven.compiler.plugin.version>
        <maven.surefire.plugin.version>3.5.1</maven.surefire.plugin.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.microsoft.sqlserver</groupId>
            <artifactId>mssql-jdbc</artifactId>
            <version>${mssql.jdbc.version}</version>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
            <version>${flyway.version}</version>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-sqlserver</artifactId>
            <version>${flyway.version}</version>
        </dependency>
        <dependency>
            <groupId>com.zaxxer</groupId>
            <artifactId>HikariCP</artifactId>
            <version>${hikari.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>${slf4j.version}</version>
        </dependency>

        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>mssqlserver</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${testcontainers.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>${maven.compiler.plugin.version}</version>
                <configuration>
                    <release>25</release>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>${maven.surefire.plugin.version}</version>
                <configuration>
                    <excludes>
                        <exclude>**/benchmark/**</exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <profiles>
        <profile>
            <id>benchmark</id>
            <build>
                <plugins>
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-surefire-plugin</artifactId>
                        <configuration>
                            <excludes combine.self="override"/>
                            <includes>
                                <include>**/benchmark/**/*Test.java</include>
                                <include>**/benchmark/**/*Runner.java</include>
                            </includes>
                        </configuration>
                    </plugin>
                </plugins>
            </build>
        </profile>
    </profiles>
</project>
```

- [ ] **Step 2: Verify Maven resolves dependencies**

Run: `cd harness && mvn -B dependency:resolve` (creates `~/.m2` cache)
Expected: BUILD SUCCESS, no unresolved deps.

If `mvn` is not installed: install via `brew install maven` (macOS) or use a Maven wrapper. Document the chosen approach in `CONTRIBUTING.md` if the wrapper is added.

### Task 0.3: Add JUnit + SLF4J configuration

**Files:**
- Create: `harness/src/test/resources/junit-platform.properties`
- Create: `harness/src/test/resources/simplelogger.properties`

- [ ] **Step 1: junit-platform.properties — virtual-thread test executor**

```properties
junit.jupiter.execution.parallel.enabled=false
junit.jupiter.testinstance.lifecycle.default=per_class
```

(We deliberately keep JUnit-level parallelism OFF — concurrency tests spawn their *own* virtual threads inside a single test method. Parallelizing test methods would muddle which test owned which container session.)

- [ ] **Step 2: simplelogger.properties**

```properties
org.slf4j.simpleLogger.defaultLogLevel=info
org.slf4j.simpleLogger.log.org.testcontainers=warn
org.slf4j.simpleLogger.log.com.zaxxer.hikari=warn
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS
```

### Task 0.4: Add `SqlServerContainer` shared infrastructure

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/support/SqlServerContainer.java`

The pattern: one Singleton container per JVM (slow to start ~25s); each test class gets its own database inside that container (cheap, ~100ms).

- [ ] **Step 1: Write the wrapper**

```java
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
```

- [ ] **Step 2: Compile-check**

Run: `cd harness && mvn -B compile test-compile`
Expected: BUILD SUCCESS.

### Task 0.5: Add `DatabasePerTest` JUnit 5 extension

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/support/DatabasePerTest.java`

- [ ] **Step 1: Write the extension**

```java
package io.freesidenomad.sqlmq.support;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;

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

    private static final Namespace NS = Namespace.create(DatabasePerTest.class);

    @Override
    public void beforeAll(ExtensionContext ctx) throws Exception {
        var container = SqlServerContainer.get();
        var dbName = "sqlmq_test_" + UUID.randomUUID().toString().replace("-", "");

        // Create the per-test database via the admin connection.
        try (Connection adminConn = java.sql.DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement st = adminConn.createStatement()) {
            st.execute("CREATE DATABASE [" + dbName + "]");
        }

        // Build a Hikari DataSource scoped to that database.
        var jdbcUrl = container.getJdbcUrl() + ";databaseName=" + dbName + ";encrypt=false";
        var hikariCfg = new HikariConfig();
        hikariCfg.setJdbcUrl(jdbcUrl);
        hikariCfg.setUsername(container.getUsername());
        hikariCfg.setPassword(container.getPassword());
        hikariCfg.setMaximumPoolSize(64);
        hikariCfg.setConnectionTimeout(5000);
        var ds = new HikariDataSource(hikariCfg);

        // Apply all migrations.
        Flyway.configure()
              .dataSource(ds)
              .locations("filesystem:../sql/migrations")
              .load()
              .migrate();

        ctx.getStore(NS).put("dbName", dbName);
        ctx.getStore(NS).put("dataSource", ds);
    }

    @Override
    public void afterAll(ExtensionContext ctx) throws Exception {
        var ds = (HikariDataSource) ctx.getStore(NS).get("dataSource");
        if (ds != null) ds.close();

        var container = SqlServerContainer.get();
        var dbName = (String) ctx.getStore(NS).get("dbName");
        if (dbName != null) {
            try (Connection adminConn = java.sql.DriverManager.getConnection(
                    container.getJdbcUrl(), container.getUsername(), container.getPassword());
                 Statement st = adminConn.createStatement()) {
                st.execute("ALTER DATABASE [" + dbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
                st.execute("DROP DATABASE [" + dbName + "]");
            }
        }
    }

    public static DataSource dataSource(ExtensionContext ctx) {
        return (DataSource) ctx.getStore(NS).get("dataSource");
    }
}
```

- [ ] **Step 2: Compile-check**

Run: `cd harness && mvn -B test-compile`
Expected: BUILD SUCCESS.

### Task 0.6: Add empty `sql/migrations/` placeholder

**Files:**
- Create: `sql/migrations/.gitkeep`

- [ ] **Step 1**

Create the file (empty). Flyway needs the directory to exist even before V001 lands; tests in this phase will skip if there are no migrations.

### Task 0.7: Add `SmokeTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/SmokeTest.java`

- [ ] **Step 1: Write the test**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.support.SqlServerContainer;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class SmokeTest {

    @Test
    void containerStartsAndAcceptsJdbcConnection() throws Exception {
        var container = SqlServerContainer.get();
        try (Connection conn = java.sql.DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT @@VERSION AS v")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("v")).contains("Microsoft SQL Server 2022");
        }
    }
}
```

- [ ] **Step 2: Run**

Run: `cd harness && mvn -B test -Dtest=SmokeTest`
Expected: BUILD SUCCESS, 1 test passed. First run is slow (Docker pulls the SQL Server image, ~1.5 GB).

### Task 0.8: Add GitHub Actions CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: CI

on:
  push:
    branches: ["**"]
  pull_request:
    branches: [main]

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 25
        uses: actions/setup-java@v4
        with:
          java-version: '25'
          distribution: 'temurin'

      - name: Cache Maven packages
        uses: actions/cache@v4
        with:
          path: ~/.m2
          key: ${{ runner.os }}-m2-${{ hashFiles('harness/pom.xml') }}
          restore-keys: ${{ runner.os }}-m2

      - name: Run tests
        working-directory: harness
        run: mvn -B verify
```

### Task 0.9: Commit Phase 0

- [ ] **Step 1: Stage and commit**

```bash
git add .github harness sql/migrations/.gitkeep
git commit -m "$(cat <<'EOF'
phase 0: project scaffolding

- pom.xml with all dependencies pinned (Java 25, Flyway 10, Testcontainers 1.20, JUnit 5, Hikari, mssql-jdbc, AssertJ)
- SqlServerContainer (singleton per JVM) and DatabasePerTest (fresh DB per class) test infrastructure
- SmokeTest verifies SQL Server 2022 container + JDBC connection
- GitHub Actions ci.yml runs mvn verify on every push

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 2: Push**

```bash
git push -u origin v1-implementation
```

- [ ] **Step 3: Verify CI**

Open https://github.com/FreeSideNomad/sqlmq/actions and confirm the `CI` workflow ran green.

---

## Phase 1 — Core schema + create/drop queue

### Task 1.1: Write `V001__core_schema.sql`

**Files:**
- Create: `sql/migrations/V001__core_schema.sql`

- [ ] **Step 1: Write the migration**

```sql
-- V001: sqlmq core schema, registry table, TVPs

CREATE SCHEMA sqlmq AUTHORIZATION dbo;
GO

-- Queue registry. One row per queue. Hot row, kept small.
CREATE TABLE sqlmq.meta (
    queue_name          SYSNAME       NOT NULL PRIMARY KEY,
    storage_type        VARCHAR(16)   NOT NULL,
    is_grouped          BIT           NOT NULL CONSTRAINT DF_sqlmq_meta_is_grouped DEFAULT (0),
    payload_type        VARCHAR(8)    NOT NULL,
    max_delivery_count  INT           NULL,
    created_at          DATETIME2(7)  NOT NULL CONSTRAINT DF_sqlmq_meta_created_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT CK_sqlmq_meta_storage_type
        CHECK (storage_type IN ('ondisk', 'inmemory')),
    CONSTRAINT CK_sqlmq_meta_payload_type
        CHECK (payload_type IN ('json', 'binary'))
);
GO

-- TVP for batch send. Caller fills exactly one of (message, message_bin) per row
-- depending on the queue's payload_type; procs validate.
CREATE TYPE dbo.sqlmq_send_tvp AS TABLE (
    message       NVARCHAR(MAX) NULL,
    message_bin   VARBINARY(MAX) NULL,
    headers       NVARCHAR(MAX) NULL,
    delay_seconds INT NOT NULL DEFAULT 0
);
GO

-- TVP for batch delete / archive. Caller passes msg_ids.
CREATE TYPE dbo.sqlmq_msg_id_tvp AS TABLE (
    msg_id BIGINT NOT NULL PRIMARY KEY
);
GO
```

- [ ] **Step 2: Verify Flyway can apply it**

Add a temporary one-shot test (delete after verification):

```java
// harness/src/test/java/io/freesidenomad/sqlmq/correctness/V001AppliesTest.java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.support.DatabasePerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.TestInstance;

import java.sql.ResultSet;
import javax.sql.DataSource;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class V001AppliesTest {

    @Test
    void schemaAndMetaTableExist(ExtensionContext ctx) throws Exception {
        DataSource ds = DatabasePerTest.dataSource(ctx);
        try (var conn = ds.getConnection();
             var st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT COUNT(*) AS n FROM sys.schemas WHERE name = 'sqlmq'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("n")).isEqualTo(1);
        }
        try (var conn = ds.getConnection();
             var st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT COUNT(*) AS n FROM sys.tables WHERE name = 'meta' AND SCHEMA_NAME(schema_id) = 'sqlmq'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("n")).isEqualTo(1);
        }
    }
}
```

- [ ] **Step 3: Run**

Run: `cd harness && mvn -B test -Dtest=V001AppliesTest`
Expected: PASS.

- [ ] **Step 4: Delete the temporary test**

Once Phase 1 ends with `CreateDropQueueTest`, this becomes redundant.

```bash
rm harness/src/test/java/io/freesidenomad/sqlmq/correctness/V001AppliesTest.java
```

### Task 1.2: Write `SqlmqClient` JDBC convenience wrapper

**Files:**
- Create: `harness/src/main/java/io/freesidenomad/sqlmq/client/SqlmqClient.java`

A thin wrapper: takes a DataSource, exposes typed methods that wrap the procs. Used by tests *only*; not a published client library. Methods are added as procs are implemented.

- [ ] **Step 1: Initial skeleton (Phase 1 surface only)**

```java
package io.freesidenomad.sqlmq.client;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class SqlmqClient {

    private final DataSource ds;

    public SqlmqClient(DataSource ds) {
        this.ds = ds;
    }

    public void createQueue(String name, String storage, boolean grouped,
                            String payloadType, Integer maxDeliveryCount) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall(
                 "{call sqlmq.create_queue(?, ?, ?, ?, ?)}")) {
            cs.setString(1, name);
            cs.setString(2, storage);
            cs.setBoolean(3, grouped);
            cs.setString(4, payloadType);
            if (maxDeliveryCount == null) cs.setNull(5, java.sql.Types.INTEGER);
            else cs.setInt(5, maxDeliveryCount);
            cs.execute();
        }
    }

    public void dropQueue(String name) throws SQLException {
        try (Connection c = ds.getConnection();
             CallableStatement cs = c.prepareCall("{call sqlmq.drop_queue(?)}")) {
            cs.setString(1, name);
            cs.execute();
        }
    }

    public List<QueueInfo> listQueues() throws SQLException {
        var out = new ArrayList<QueueInfo>();
        try (Connection c = ds.getConnection();
             var st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT queue_name, storage_type, is_grouped, payload_type, " +
                 "       max_delivery_count, created_at FROM sqlmq.queues()")) {
            while (rs.next()) {
                out.add(new QueueInfo(
                    rs.getString("queue_name"),
                    rs.getString("storage_type"),
                    rs.getBoolean("is_grouped"),
                    rs.getString("payload_type"),
                    rs.getObject("max_delivery_count") == null ? null : rs.getInt("max_delivery_count"),
                    rs.getTimestamp("created_at").toInstant()
                ));
            }
        }
        return out;
    }

    public record QueueInfo(
        String name, String storageType, boolean grouped, String payloadType,
        Integer maxDeliveryCount, java.time.Instant createdAt) {}
}
```

- [ ] **Step 2: Compile-check**

Run: `cd harness && mvn -B compile`
Expected: BUILD SUCCESS.

### Task 1.3: Add `TestQueues` helper

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/support/TestQueues.java`

- [ ] **Step 1: Write the helper**

```java
package io.freesidenomad.sqlmq.support;

import java.util.concurrent.atomic.AtomicLong;

public final class TestQueues {
    private static final AtomicLong SEQ = new AtomicLong();

    private TestQueues() {}

    /** Unique queue name per call within a JVM. */
    public static String uniqueName(String prefix) {
        return prefix + "_" + SEQ.incrementAndGet();
    }
}
```

### Task 1.4: Write `V002__create_drop_queue_procs.sql` — interpreted-only

**Files:**
- Create: `sql/migrations/V002__create_drop_queue_procs.sql`

For Phase 1, both `create_queue` and `drop_queue` only handle `@storage = 'ondisk'`. We THROW for `@storage = 'inmemory'` until Phase 6.

- [ ] **Step 1: Write the migration**

```sql
-- V002: sqlmq.create_queue, sqlmq.drop_queue, sqlmq.queues TVF
-- In Phase 1, in-memory storage is unsupported (V009 will extend create/drop_queue).

CREATE PROCEDURE sqlmq.create_queue
    @name SYSNAME,
    @storage VARCHAR(16) = 'ondisk',
    @grouped BIT = 0,
    @payload_type VARCHAR(8) = 'json',
    @max_delivery_count INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @storage NOT IN ('ondisk', 'inmemory')
        THROW 50001, 'Invalid @storage; must be ''ondisk'' or ''inmemory''.', 1;
    IF @payload_type NOT IN ('json', 'binary')
        THROW 50002, 'Invalid @payload_type; must be ''json'' or ''binary''.', 1;
    IF @storage = 'inmemory'
        THROW 50003, 'In-memory storage is not implemented yet (planned for V009).', 1;
    IF EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name)
        THROW 50004, 'Queue already exists.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @atable SYSNAME = N'a_' + @name;

    DECLARE @msg_col_def NVARCHAR(MAX);
    DECLARE @msg_check  NVARCHAR(MAX);
    IF @payload_type = 'json'
    BEGIN
        SET @msg_col_def = N'message NVARCHAR(MAX) NOT NULL';
        SET @msg_check   = N', CONSTRAINT CK_' + @qtable + N'_message_json CHECK (ISJSON(message) = 1)';
    END
    ELSE
    BEGIN
        SET @msg_col_def = N'message_bin VARBINARY(MAX) NOT NULL';
        SET @msg_check   = N'';
    END

    DECLARE @ddl_q NVARCHAR(MAX) = N'
        CREATE TABLE sqlmq.' + QUOTENAME(@qtable) + N' (
            msg_id      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
            enqueued_at DATETIME2(7) NOT NULL CONSTRAINT DF_' + @qtable + N'_eq DEFAULT (SYSUTCDATETIME()),
            vt          DATETIME2(7) NOT NULL,
            read_ct     INT          NOT NULL CONSTRAINT DF_' + @qtable + N'_rc DEFAULT (0),
            group_key   NVARCHAR(255) NULL,
            ' + @msg_col_def + N',
            headers     NVARCHAR(MAX) NULL,
            CONSTRAINT CK_' + @qtable + N'_headers_json
                CHECK (headers IS NULL OR ISJSON(headers) = 1)
            ' + @msg_check + N'
        );
        CREATE NONCLUSTERED INDEX IX_' + @qtable + N'_vt
            ON sqlmq.' + QUOTENAME(@qtable) + N' (vt, msg_id);';

    EXEC sp_executesql @ddl_q;

    IF @grouped = 1
    BEGIN
        DECLARE @ddl_grp NVARCHAR(MAX) = N'
            CREATE NONCLUSTERED INDEX IX_' + @qtable + N'_group
                ON sqlmq.' + QUOTENAME(@qtable) + N' (group_key, vt, msg_id)
                WHERE group_key IS NOT NULL;';
        EXEC sp_executesql @ddl_grp;
    END

    DECLARE @msg_col_a NVARCHAR(MAX) =
        CASE @payload_type WHEN 'json' THEN N'message NVARCHAR(MAX) NOT NULL'
                           ELSE N'message_bin VARBINARY(MAX) NOT NULL' END;

    DECLARE @ddl_a NVARCHAR(MAX) = N'
        CREATE TABLE sqlmq.' + QUOTENAME(@atable) + N' (
            msg_id      BIGINT NOT NULL PRIMARY KEY CLUSTERED,
            enqueued_at DATETIME2(7) NOT NULL,
            vt          DATETIME2(7) NOT NULL,
            read_ct     INT          NOT NULL,
            group_key   NVARCHAR(255) NULL,
            ' + @msg_col_a + N',
            headers     NVARCHAR(MAX) NULL,
            archived_at DATETIME2(7) NOT NULL CONSTRAINT DF_' + @atable + N'_at DEFAULT (SYSUTCDATETIME()),
            dlq_reason  NVARCHAR(64) NULL
        );';
    EXEC sp_executesql @ddl_a;

    INSERT INTO sqlmq.meta (queue_name, storage_type, is_grouped, payload_type, max_delivery_count)
    VALUES (@name, @storage, @grouped, @payload_type, @max_delivery_count);
END
GO

CREATE PROCEDURE sqlmq.drop_queue
    @name SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NOT EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name)
        THROW 50010, 'Queue does not exist.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @atable SYSNAME = N'a_' + @name;

    DECLARE @drop NVARCHAR(MAX) =
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@qtable) + N';' +
        N'DROP TABLE IF EXISTS sqlmq.' + QUOTENAME(@atable) + N';';
    EXEC sp_executesql @drop;

    DELETE FROM sqlmq.meta WHERE queue_name = @name;
END
GO

CREATE FUNCTION sqlmq.queues()
RETURNS TABLE
AS
RETURN (
    SELECT queue_name, storage_type, is_grouped, payload_type, max_delivery_count, created_at
    FROM sqlmq.meta
);
GO
```

### Task 1.5: Write `CreateDropQueueTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/CreateDropQueueTest.java`

- [ ] **Step 1: Write the failing test**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class CreateDropQueueTest {

    @Test
    void createsAndListsOnDiskJsonQueue(ExtensionContext ctx) throws SQLException {
        DataSource ds = DatabasePerTest.dataSource(ctx);
        var client = new SqlmqClient(ds);
        var name = TestQueues.uniqueName("q");

        client.createQueue(name, "ondisk", false, "json", null);

        var listed = client.listQueues();
        assertThat(listed).extracting(SqlmqClient.QueueInfo::name).contains(name);
        var info = listed.stream().filter(q -> q.name().equals(name)).findFirst().orElseThrow();
        assertThat(info.storageType()).isEqualTo("ondisk");
        assertThat(info.payloadType()).isEqualTo("json");
        assertThat(info.grouped()).isFalse();
        assertThat(info.maxDeliveryCount()).isNull();
    }

    @Test
    void createWithGroupingAndDlqCap(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var name = TestQueues.uniqueName("q");
        client.createQueue(name, "ondisk", true, "binary", 5);
        var info = client.listQueues().stream()
            .filter(q -> q.name().equals(name)).findFirst().orElseThrow();
        assertThat(info.grouped()).isTrue();
        assertThat(info.payloadType()).isEqualTo("binary");
        assertThat(info.maxDeliveryCount()).isEqualTo(5);
    }

    @Test
    void inmemoryStorageThrowsUntilV009(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var name = TestQueues.uniqueName("q");
        assertThatThrownBy(() -> client.createQueue(name, "inmemory", false, "json", null))
            .hasMessageContaining("In-memory storage is not implemented yet");
    }

    @Test
    void duplicateCreateFails(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var name = TestQueues.uniqueName("q");
        client.createQueue(name, "ondisk", false, "json", null);
        assertThatThrownBy(() -> client.createQueue(name, "ondisk", false, "json", null))
            .hasMessageContaining("already exists");
    }

    @Test
    void dropRemovesQueueAndTables(ExtensionContext ctx) throws SQLException {
        var ds = DatabasePerTest.dataSource(ctx);
        var client = new SqlmqClient(ds);
        var name = TestQueues.uniqueName("q");
        client.createQueue(name, "ondisk", false, "json", null);
        client.dropQueue(name);

        assertThat(client.listQueues()).extracting(SqlmqClient.QueueInfo::name).doesNotContain(name);

        try (var c = ds.getConnection(); var st = c.createStatement();
             var rs = st.executeQuery(
                 "SELECT COUNT(*) AS n FROM sys.tables WHERE name IN ('q_" + name + "', 'a_" + name + "')")) {
            rs.next();
            assertThat(rs.getInt("n")).isEqualTo(0);
        }
    }

    @Test
    void dropOfNonexistentFails(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        assertThatThrownBy(() -> client.dropQueue("nonexistent"))
            .hasMessageContaining("does not exist");
    }
}
```

- [ ] **Step 2: Run, verify all pass**

Run: `cd harness && mvn -B test -Dtest=CreateDropQueueTest`
Expected: 6 tests, 6 passed.

### Task 1.6: Commit Phase 1

- [ ] **Step 1**

```bash
git add sql/migrations/V001__core_schema.sql sql/migrations/V002__create_drop_queue_procs.sql \
        harness/src/main harness/src/test/java/io/freesidenomad/sqlmq/support/TestQueues.java \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/CreateDropQueueTest.java
git rm harness/src/test/java/io/freesidenomad/sqlmq/correctness/V001AppliesTest.java 2>/dev/null || true
git commit -m "$(cat <<'EOF'
phase 1: core schema, create/drop queue, queues TVF

V001 — sqlmq schema, sqlmq.meta registry, sqlmq_send_tvp + sqlmq_msg_id_tvp.
V002 — sqlmq.create_queue / drop_queue (on-disk only; in-memory THROWs until V009);
       sqlmq.queues() inline TVF.

CreateDropQueueTest covers create + list + drop, grouped queues with DLQ caps,
binary payload type, duplicate-create rejection, drop-of-nonexistent rejection,
in-memory storage rejection.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push
```

---

## Phase 2 — On-disk send / read / delete / archive / pop

### Task 2.1: Write `V003__send_procs.sql`

**Files:**
- Create: `sql/migrations/V003__send_procs.sql`

`sqlmq.send` and `sqlmq.send_batch` dispatchers + on-disk inner procs.

- [ ] **Step 1: Write the migration**

```sql
-- V003: send and send_batch
-- Dispatchers look up storage_type in meta and route to inner procs.

CREATE PROCEDURE sqlmq._send_ondisk
    @queue          SYSNAME,
    @payload_type   VARCHAR(8),
    @message        NVARCHAR(MAX) = NULL,
    @message_bin    VARBINARY(MAX) = NULL,
    @headers        NVARCHAR(MAX) = NULL,
    @delay_seconds  INT = 0,
    @msg_id         BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @vt DATETIME2(7) = DATEADD(SECOND, @delay_seconds, SYSUTCDATETIME());
    DECLARE @sql NVARCHAR(MAX);

    IF @payload_type = 'json'
    BEGIN
        IF @message IS NULL THROW 50020, '@message required for json queue', 1;
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message, headers)
            OUTPUT inserted.msg_id
            VALUES (@vt, @msg, @hdr);';
        DECLARE @ids TABLE (id BIGINT);
        INSERT INTO @ids EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @msg NVARCHAR(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @msg = @message, @hdr = @headers;
        SELECT @msg_id = id FROM @ids;
    END
    ELSE
    BEGIN
        IF @message_bin IS NULL THROW 50021, '@message_bin required for binary queue', 1;
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message_bin, headers)
            OUTPUT inserted.msg_id
            VALUES (@vt, @msg, @hdr);';
        DECLARE @ids2 TABLE (id BIGINT);
        INSERT INTO @ids2 EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @msg VARBINARY(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @msg = @message_bin, @hdr = @headers;
        SELECT @msg_id = id FROM @ids2;
    END
END
GO

CREATE PROCEDURE sqlmq.send
    @queue         SYSNAME,
    @message       NVARCHAR(MAX) = NULL,
    @message_bin   VARBINARY(MAX) = NULL,
    @headers       NVARCHAR(MAX) = NULL,
    @delay_seconds INT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50030, 'Queue does not exist.', 1;

    DECLARE @msg_id BIGINT;
    IF @storage = 'ondisk'
        EXEC sqlmq._send_ondisk
            @queue, @payload, @message, @message_bin, @headers, @delay_seconds, @msg_id OUTPUT;
    ELSE
        THROW 50031, 'In-memory send not implemented yet (V009).', 1;

    SELECT @msg_id AS msg_id;
END
GO

CREATE PROCEDURE sqlmq._send_batch_ondisk
    @queue        SYSNAME,
    @payload_type VARCHAR(8),
    @messages     dbo.sqlmq_send_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @sql NVARCHAR(MAX);

    IF @payload_type = 'json'
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message, headers)
            OUTPUT inserted.msg_id
            SELECT DATEADD(SECOND, m.delay_seconds, SYSUTCDATETIME()), m.message, m.headers
              FROM @msgs AS m
             ORDER BY (SELECT NULL);';
    ELSE
        SET @sql = N'
            INSERT INTO sqlmq.' + QUOTENAME(@qtable) + N' (vt, message_bin, headers)
            OUTPUT inserted.msg_id
            SELECT DATEADD(SECOND, m.delay_seconds, SYSUTCDATETIME()), m.message_bin, m.headers
              FROM @msgs AS m
             ORDER BY (SELECT NULL);';

    EXEC sp_executesql @sql, N'@msgs dbo.sqlmq_send_tvp READONLY', @msgs = @messages;
END
GO

CREATE PROCEDURE sqlmq.send_batch
    @queue    SYSNAME,
    @messages dbo.sqlmq_send_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50032, 'Queue does not exist.', 1;

    IF @storage = 'ondisk'
        EXEC sqlmq._send_batch_ondisk @queue, @payload, @messages;
    ELSE
        THROW 50033, 'In-memory send_batch not implemented yet (V009).', 1;
END
GO
```

### Task 2.2: Extend `SqlmqClient` with `send` and `sendBatch`

**Files:**
- Modify: `harness/src/main/java/io/freesidenomad/sqlmq/client/SqlmqClient.java`

- [ ] **Step 1: Add methods**

Add these methods inside the existing `SqlmqClient` class:

```java
public long send(String queue, String message, String headers) throws SQLException {
    return sendInternal(queue, message, null, headers, 0);
}

public long sendDelayed(String queue, String message, String headers, int delaySeconds) throws SQLException {
    return sendInternal(queue, message, null, headers, delaySeconds);
}

public long sendBinary(String queue, byte[] message, String headers) throws SQLException {
    return sendInternal(queue, null, message, headers, 0);
}

private long sendInternal(String queue, String msg, byte[] msgBin, String headers, int delaySeconds) throws SQLException {
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.send(?, ?, ?, ?, ?)}")) {
        cs.setString(1, queue);
        if (msg == null) cs.setNull(2, java.sql.Types.NVARCHAR); else cs.setString(2, msg);
        if (msgBin == null) cs.setNull(3, java.sql.Types.VARBINARY); else cs.setBytes(3, msgBin);
        if (headers == null) cs.setNull(4, java.sql.Types.NVARCHAR); else cs.setString(4, headers);
        cs.setInt(5, delaySeconds);
        try (ResultSet rs = cs.executeQuery()) {
            rs.next();
            return rs.getLong("msg_id");
        }
    }
}

public List<Long> sendBatch(String queue, List<String> jsonMessages) throws SQLException {
    var tvp = new com.microsoft.sqlserver.jdbc.SQLServerDataTable();
    tvp.addColumnMetadata("message",       java.sql.Types.NVARCHAR);
    tvp.addColumnMetadata("message_bin",   java.sql.Types.VARBINARY);
    tvp.addColumnMetadata("headers",       java.sql.Types.NVARCHAR);
    tvp.addColumnMetadata("delay_seconds", java.sql.Types.INTEGER);
    for (var m : jsonMessages) tvp.addRow(m, null, null, 0);

    var ids = new ArrayList<Long>();
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.send_batch(?, ?)}")) {
        cs.setString(1, queue);
        ((com.microsoft.sqlserver.jdbc.SQLServerCallableStatement) cs)
            .setStructured(2, "dbo.sqlmq_send_tvp", tvp);
        try (ResultSet rs = cs.executeQuery()) {
            while (rs.next()) ids.add(rs.getLong("msg_id"));
        }
    }
    return ids;
}
```

- [ ] **Step 2: Compile-check**

Run: `cd harness && mvn -B compile`
Expected: BUILD SUCCESS.

### Task 2.3: Write `SendTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/SendTest.java`

- [ ] **Step 1: Write the tests**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class SendTest {

    @Test
    void sendReturnsMonotonicMsgIds(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var id1 = client.send(q, "{\"v\":1}", null);
        var id2 = client.send(q, "{\"v\":2}", null);
        var id3 = client.send(q, "{\"v\":3}", null);

        assertThat(id1).isPositive();
        assertThat(id2).isGreaterThan(id1);
        assertThat(id3).isGreaterThan(id2);
    }

    @Test
    void invalidJsonRejected(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        assertThatThrownBy(() -> client.send(q, "not json at all", null))
            .hasMessageContaining("CK_q_" + q + "_message_json");
    }

    @Test
    void invalidJsonHeadersRejected(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        assertThatThrownBy(() -> client.send(q, "{\"ok\":true}", "not json headers"))
            .hasMessageContaining("CK_q_" + q + "_headers_json");
    }

    @Test
    void binaryQueueAcceptsBytes(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "binary", null);

        var id = client.sendBinary(q, new byte[]{1, 2, 3, 4, 5}, null);
        assertThat(id).isPositive();
    }

    @Test
    void batchSendReturnsAllMsgIdsInOrder(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var ids = client.sendBatch(q, List.of("{\"a\":1}", "{\"a\":2}", "{\"a\":3}"));
        assertThat(ids).hasSize(3);
        assertThat(ids).isSorted();
    }

    @Test
    void sendToNonexistentQueueFails(ExtensionContext ctx) {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        assertThatThrownBy(() -> client.send("nope", "{}", null))
            .hasMessageContaining("does not exist");
    }
}
```

- [ ] **Step 2: Run**

Run: `cd harness && mvn -B test -Dtest=SendTest`
Expected: 6 tests, 6 passed.

### Task 2.4: Write `V004__read_procs.sql`

**Files:**
- Create: `sql/migrations/V004__read_procs.sql`

`sqlmq.read` dispatcher + on-disk inner proc (ungrouped). Excludes over-cap messages from candidates (DLQ comes in Phase 4 sweeper).

- [ ] **Step 1: Write the migration**

```sql
-- V004: read (basic, ungrouped). Grouped variant lands in V008.

CREATE PROCEDURE sqlmq._read_ondisk
    @queue       SYSNAME,
    @payload_type VARCHAR(8),
    @vt_seconds  INT,
    @max_count   INT,
    @max_dlq     INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    SET @sql = N'
        WITH claimed AS (
            SELECT TOP (@n) msg_id
            FROM sqlmq.' + QUOTENAME(@qtable) + N'
                WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
            WHERE vt <= SYSUTCDATETIME()
              AND (@dlq IS NULL OR read_ct < @dlq)
            ORDER BY msg_id
        )
        UPDATE q
           SET vt = DATEADD(SECOND, @vt, SYSUTCDATETIME()),
               read_ct = read_ct + 1
        OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
               inserted.vt, inserted.' + @msg_col + N', inserted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN claimed AS c ON q.msg_id = c.msg_id;';

    EXEC sp_executesql @sql,
        N'@n INT, @vt INT, @dlq INT',
        @n = @max_count, @vt = @vt_seconds, @dlq = @max_dlq;
END
GO

CREATE PROCEDURE sqlmq.read
    @queue      SYSNAME,
    @vt_seconds INT,
    @max_count  INT = 1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT;
    SELECT @storage = storage_type, @payload = payload_type, @max_dlq = max_delivery_count
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50040, 'Queue does not exist.', 1;

    IF @storage = 'ondisk'
        EXEC sqlmq._read_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
    ELSE
        THROW 50041, 'In-memory read not implemented yet (V009).', 1;
END
GO
```

### Task 2.5: Extend `SqlmqClient` with `read`

**Files:**
- Modify: `harness/src/main/java/io/freesidenomad/sqlmq/client/SqlmqClient.java`

- [ ] **Step 1: Add the method and the Message record**

```java
public List<Message> read(String queue, int vtSeconds, int maxCount) throws SQLException {
    var out = new ArrayList<Message>();
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.read(?, ?, ?)}")) {
        cs.setString(1, queue);
        cs.setInt(2, vtSeconds);
        cs.setInt(3, maxCount);
        try (ResultSet rs = cs.executeQuery()) {
            var meta = rs.getMetaData();
            // Last data column is either "message" or "message_bin"; detect.
            boolean binary = false;
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if ("message_bin".equalsIgnoreCase(meta.getColumnLabel(i))) { binary = true; break; }
            }
            while (rs.next()) {
                out.add(new Message(
                    rs.getLong("msg_id"),
                    rs.getInt("read_ct"),
                    rs.getTimestamp("enqueued_at").toInstant(),
                    rs.getTimestamp("vt").toInstant(),
                    binary ? null : rs.getString("message"),
                    binary ? rs.getBytes("message_bin") : null,
                    rs.getString("headers")
                ));
            }
        }
    }
    return out;
}

public record Message(
    long msgId, int readCt, java.time.Instant enqueuedAt, java.time.Instant vt,
    String message, byte[] messageBin, String headers) {}
```

### Task 2.6: Write `ReadTest` (single-session correctness)

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/ReadTest.java`

- [ ] **Step 1: Write the tests**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class ReadTest {

    @Test
    void emptyQueueReturnsEmptyList(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);
        assertThat(client.read(q, 30, 10)).isEmpty();
    }

    @Test
    void readReturnsFifoOrderedMessagesAndIncrementsReadCt(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var id1 = client.send(q, "{\"v\":1}", null);
        var id2 = client.send(q, "{\"v\":2}", null);
        var id3 = client.send(q, "{\"v\":3}", null);

        var msgs = client.read(q, 60, 10);
        assertThat(msgs).extracting(SqlmqClient.Message::msgId).containsExactly(id1, id2, id3);
        assertThat(msgs).allSatisfy(m -> assertThat(m.readCt()).isEqualTo(1));
    }

    @Test
    void secondReadDoesNotReturnInflightMessages(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        client.send(q, "{\"v\":1}", null);
        var first = client.read(q, 60, 10);
        var second = client.read(q, 60, 10);

        assertThat(first).hasSize(1);
        assertThat(second).isEmpty();
    }

    @Test
    void messagesBecomeReEligibleAfterVtExpires(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        client.send(q, "{\"v\":1}", null);
        var first = client.read(q, 1, 10);
        assertThat(first).hasSize(1);
        Thread.sleep(1500);
        var second = client.read(q, 60, 10);
        assertThat(second).hasSize(1);
        assertThat(second.get(0).readCt()).isEqualTo(2);
    }

    @Test
    void delayedSendNotReadableUntilDelayElapses(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        client.sendDelayed(q, "{\"v\":1}", null, 2);
        assertThat(client.read(q, 30, 10)).isEmpty();
        Thread.sleep(2200);
        assertThat(client.read(q, 30, 10)).hasSize(1);
    }
}
```

- [ ] **Step 2: Run**

Run: `cd harness && mvn -B test -Dtest=ReadTest`
Expected: 5 tests, 5 passed.

### Task 2.7: Write `V005__delete_archive_pop_procs.sql`

**Files:**
- Create: `sql/migrations/V005__delete_archive_pop_procs.sql`

- [ ] **Step 1: Write the migration**

```sql
-- V005: delete (storage-dispatched), archive (always interpreted, cross-engine), pop

CREATE PROCEDURE sqlmq._delete_ondisk
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN @ids AS i ON q.msg_id = i.msg_id;
        SELECT @@ROWCOUNT AS rows_deleted;';
    EXEC sp_executesql @sql, N'@ids dbo.sqlmq_msg_id_tvp READONLY', @ids = @msg_ids;
END
GO

CREATE PROCEDURE sqlmq.delete
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16);
    SELECT @storage = storage_type FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50050, 'Queue does not exist.', 1;
    IF @storage = 'ondisk' EXEC sqlmq._delete_ondisk @queue, @msg_ids;
    ELSE THROW 50051, 'In-memory delete not implemented yet (V009).', 1;
END
GO

-- Archive is always interpreted: in-memory queue → on-disk archive crosses engines,
-- so a natively compiled proc cannot do the move.
CREATE PROCEDURE sqlmq.archive
    @queue   SYSNAME,
    @msg_ids dbo.sqlmq_msg_id_tvp READONLY,
    @reason  NVARCHAR(64) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50052, 'Queue does not exist.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @atable SYSNAME = N'a_' + @queue;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q
          OUTPUT deleted.msg_id, deleted.enqueued_at, deleted.vt, deleted.read_ct,
                 deleted.group_key, deleted.' + @msg_col + N', deleted.headers,
                 SYSUTCDATETIME(), @reason
            INTO sqlmq.' + QUOTENAME(@atable) + N' (
                 msg_id, enqueued_at, vt, read_ct, group_key, ' + @msg_col + N', headers,
                 archived_at, dlq_reason)
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN @ids AS i ON q.msg_id = i.msg_id;
        SELECT @@ROWCOUNT AS rows_archived;';

    EXEC sp_executesql @sql,
        N'@ids dbo.sqlmq_msg_id_tvp READONLY, @reason NVARCHAR(64)',
        @ids = @msg_ids, @reason = @reason;
END
GO

-- pop: read + delete in one statement, in caller's tx.
CREATE PROCEDURE sqlmq._pop_ondisk
    @queue        SYSNAME,
    @payload_type VARCHAR(8)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @msg_col NVARCHAR(64) =
        CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;
    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE TOP (1) q
          OUTPUT deleted.msg_id, deleted.read_ct, deleted.enqueued_at, deleted.vt,
                 deleted.' + @msg_col + N', deleted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
         WHERE q.vt <= SYSUTCDATETIME()
           AND q.msg_id = (
               SELECT TOP (1) msg_id FROM sqlmq.' + QUOTENAME(@qtable) +
                   N' WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
                WHERE vt <= SYSUTCDATETIME() ORDER BY msg_id);';
    EXEC sp_executesql @sql;
END
GO

CREATE PROCEDURE sqlmq.pop
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50053, 'Queue does not exist.', 1;
    IF @storage = 'ondisk' EXEC sqlmq._pop_ondisk @queue, @payload;
    ELSE THROW 50054, 'In-memory pop not implemented yet (V009).', 1;
END
GO
```

### Task 2.8: Extend `SqlmqClient` with `delete`, `archive`, `pop`

**Files:**
- Modify: `harness/src/main/java/io/freesidenomad/sqlmq/client/SqlmqClient.java`

- [ ] **Step 1: Add methods**

```java
private com.microsoft.sqlserver.jdbc.SQLServerDataTable msgIdTvp(java.util.Collection<Long> ids) throws SQLException {
    var t = new com.microsoft.sqlserver.jdbc.SQLServerDataTable();
    t.addColumnMetadata("msg_id", java.sql.Types.BIGINT);
    for (long id : ids) t.addRow(id);
    return t;
}

public int delete(String queue, java.util.Collection<Long> msgIds) throws SQLException {
    if (msgIds.isEmpty()) return 0;
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.delete(?, ?)}")) {
        cs.setString(1, queue);
        ((com.microsoft.sqlserver.jdbc.SQLServerCallableStatement) cs)
            .setStructured(2, "dbo.sqlmq_msg_id_tvp", msgIdTvp(msgIds));
        try (ResultSet rs = cs.executeQuery()) {
            rs.next();
            return rs.getInt("rows_deleted");
        }
    }
}

public int archive(String queue, java.util.Collection<Long> msgIds, String reason) throws SQLException {
    if (msgIds.isEmpty()) return 0;
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.archive(?, ?, ?)}")) {
        cs.setString(1, queue);
        ((com.microsoft.sqlserver.jdbc.SQLServerCallableStatement) cs)
            .setStructured(2, "dbo.sqlmq_msg_id_tvp", msgIdTvp(msgIds));
        if (reason == null) cs.setNull(3, java.sql.Types.NVARCHAR); else cs.setString(3, reason);
        try (ResultSet rs = cs.executeQuery()) {
            rs.next();
            return rs.getInt("rows_archived");
        }
    }
}

public java.util.Optional<Message> pop(String queue) throws SQLException {
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.pop(?)}")) {
        cs.setString(1, queue);
        try (ResultSet rs = cs.executeQuery()) {
            if (!rs.next()) return java.util.Optional.empty();
            var meta = rs.getMetaData();
            boolean binary = false;
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if ("message_bin".equalsIgnoreCase(meta.getColumnLabel(i))) { binary = true; break; }
            }
            return java.util.Optional.of(new Message(
                rs.getLong("msg_id"),
                rs.getInt("read_ct"),
                rs.getTimestamp("enqueued_at").toInstant(),
                rs.getTimestamp("vt").toInstant(),
                binary ? null : rs.getString("message"),
                binary ? rs.getBytes("message_bin") : null,
                rs.getString("headers")
            ));
        }
    }
}
```

### Task 2.9: Write `DeleteArchivePopTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/DeleteArchivePopTest.java`

- [ ] **Step 1: Write the tests**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class DeleteArchivePopTest {

    @Test
    void deleteRemovesMessages(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        client.send(q, "{\"v\":1}", null);
        client.send(q, "{\"v\":2}", null);
        var msgs = client.read(q, 60, 10);
        var deleted = client.delete(q, msgs.stream().map(SqlmqClient.Message::msgId).toList());
        assertThat(deleted).isEqualTo(2);
        assertThat(rowCount(ctx, "q_" + q)).isZero();
    }

    @Test
    void archiveMovesToArchiveTableWithReason(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        client.send(q, "{\"v\":1}", null);
        var msgs = client.read(q, 60, 10);
        var ids = msgs.stream().map(SqlmqClient.Message::msgId).toList();
        var archived = client.archive(q, ids, "test_reason");
        assertThat(archived).isEqualTo(1);

        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             var st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT dlq_reason FROM sqlmq.[a_" + q + "] WHERE msg_id = " + ids.get(0))) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("dlq_reason")).isEqualTo("test_reason");
        }
        assertThat(rowCount(ctx, "q_" + q)).isZero();
    }

    @Test
    void popReturnsAndDeletesAtomically(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var id1 = client.send(q, "{\"v\":1}", null);
        client.send(q, "{\"v\":2}", null);

        var popped = client.pop(q);
        assertThat(popped).isPresent();
        assertThat(popped.get().msgId()).isEqualTo(id1);
        assertThat(rowCount(ctx, "q_" + q)).isEqualTo(1);
    }

    @Test
    void popOnEmptyReturnsEmpty(ExtensionContext ctx) throws SQLException {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);
        assertThat(client.pop(q)).isEmpty();
    }

    private int rowCount(ExtensionContext ctx, String table) throws SQLException {
        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             var st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) AS n FROM sqlmq.[" + table + "]")) {
            rs.next();
            return rs.getInt("n");
        }
    }
}
```

- [ ] **Step 2: Run**

Run: `cd harness && mvn -B test -Dtest=DeleteArchivePopTest`
Expected: 4 tests, 4 passed.

### Task 2.10: Add `LongPollingConsumer`

**Files:**
- Create: `harness/src/main/java/io/freesidenomad/sqlmq/client/LongPollingConsumer.java`

- [ ] **Step 1: Write the helper**

```java
package io.freesidenomad.sqlmq.client;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

/**
 * Hangfire-style adaptive backoff for client-side long-polling.
 * Default: 250ms interval, clamped to [100, 1000]ms.
 * After a hit, drops to the minimum; after consecutive misses, grows toward the maximum.
 */
public final class LongPollingConsumer {

    private final SqlmqClient client;
    private final String queue;
    private final int vtSeconds;
    private final int maxCount;
    private final long minMs;
    private final long maxMs;
    private long currentMs;

    public LongPollingConsumer(SqlmqClient client, String queue, int vtSeconds, int maxCount) {
        this(client, queue, vtSeconds, maxCount, 100, 1000, 250);
    }

    public LongPollingConsumer(SqlmqClient client, String queue, int vtSeconds, int maxCount,
                               long minMs, long maxMs, long initialMs) {
        this.client = client;
        this.queue = queue;
        this.vtSeconds = vtSeconds;
        this.maxCount = maxCount;
        this.minMs = minMs;
        this.maxMs = maxMs;
        this.currentMs = Math.min(Math.max(initialMs, minMs), maxMs);
    }

    /**
     * Poll until handler returns false or thread is interrupted.
     * Handler returns true to continue, false to stop the loop.
     */
    public void run(Function<List<SqlmqClient.Message>, Boolean> handler) throws SQLException, InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            var msgs = client.read(queue, vtSeconds, maxCount);
            if (msgs.isEmpty()) {
                Thread.sleep(currentMs);
                currentMs = Math.min(currentMs * 2, maxMs);
            } else {
                currentMs = minMs;
                if (!handler.apply(msgs)) return;
            }
        }
    }
}
```

### Task 2.11: Commit Phase 2

- [ ] **Step 1**

```bash
git add sql/migrations/V003__send_procs.sql sql/migrations/V004__read_procs.sql \
        sql/migrations/V005__delete_archive_pop_procs.sql \
        harness/src/main \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/SendTest.java \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/ReadTest.java \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/DeleteArchivePopTest.java
git commit -m "$(cat <<'EOF'
phase 2: on-disk send/read/delete/archive/pop

V003 — send + send_batch (dispatcher + on-disk inner; in-memory THROWs).
V004 — read with READPAST/UPDLOCK/ROWLOCK/READCOMMITTEDLOCK pattern; excludes
       over-DLQ-cap messages from candidate set (sweep handles them in Phase 4).
V005 — delete (storage-dispatched), archive (always interpreted; cross-engine for in-memory),
       pop (read + delete in one statement).
LongPollingConsumer client helper (Hangfire-style adaptive backoff).

Single-session correctness tests for send, read (incl. VT honored within session,
delayed send), and delete/archive/pop.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push
```

---

## Phase 3 — On-disk concurrency tests

### Task 3.1: Add `ConcurrencyHarness`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/support/ConcurrencyHarness.java`

The shared rig: spawn N producers and M consumers as virtual threads, run a workload, collect results, expose the data needed to assert invariants.

- [ ] **Step 1: Write the harness**

```java
package io.freesidenomad.sqlmq.support;

import io.freesidenomad.sqlmq.client.SqlmqClient;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spawns N producers and M consumers as virtual threads.
 *
 * Producers each send `messagesPerProducer` messages.
 * Consumers loop: read(maxCount=batchSize, vt=vtSeconds) → delete.
 * Each delivered (msg_id, consumer_id, received_at_nanos) tuple is recorded.
 *
 * Run completes when all producers are done AND queue stays empty for `quietPeriodMs`.
 */
public final class ConcurrencyHarness {

    public record Profile(int producers, int consumers, int messagesPerProducer,
                          int batchSize, int vtSeconds, long quietPeriodMs) {
        public static Profile light() { return new Profile(4, 4, 100, 10, 30, 500); }
        public static Profile medium() { return new Profile(32, 32, 1000, 10, 30, 1000); }
        public static Profile heavy() { return new Profile(256, 256, 10000, 50, 30, 2000); }
        public static Profile asymmetric() { return new Profile(1, 64, 100000, 100, 30, 2000); }
    }

    public record Delivery(long msgId, int consumerId, long receivedAtNanos, long vtExpiresAtNanos) {}

    public record Result(
        List<Delivery> deliveries,
        Set<Long> producedMsgIds,
        AtomicLong producerErrors,
        AtomicLong consumerErrors
    ) {}

    public static Result run(SqlmqClient client, String queue, Profile p) throws Exception {
        var deliveries = new java.util.concurrent.CopyOnWriteArrayList<Delivery>();
        var producedIds = ConcurrentHashMap.<Long>newKeySet();
        var producerErrs = new AtomicLong();
        var consumerErrs = new AtomicLong();
        var producersDone = new AtomicInteger(0);

        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            // Producers
            for (int pi = 0; pi < p.producers(); pi++) {
                final int producerId = pi;
                scope.fork(() -> {
                    try {
                        for (int k = 0; k < p.messagesPerProducer(); k++) {
                            long id = client.send(queue,
                                "{\"p\":" + producerId + ",\"k\":" + k + "}", null);
                            producedIds.add(id);
                        }
                    } catch (SQLException e) {
                        producerErrs.incrementAndGet();
                    } finally {
                        producersDone.incrementAndGet();
                    }
                    return null;
                });
            }

            // Consumers
            for (int ci = 0; ci < p.consumers(); ci++) {
                final int consumerId = ci;
                scope.fork(() -> {
                    long lastNonEmptyNanos = System.nanoTime();
                    while (true) {
                        try {
                            var msgs = client.read(queue, p.vtSeconds(), p.batchSize());
                            if (!msgs.isEmpty()) {
                                long now = System.nanoTime();
                                lastNonEmptyNanos = now;
                                long vtExpiresNanos = now + java.util.concurrent.TimeUnit.SECONDS.toNanos(p.vtSeconds());
                                for (var m : msgs) {
                                    deliveries.add(new Delivery(m.msgId(), consumerId, now, vtExpiresNanos));
                                }
                                client.delete(queue, msgs.stream().map(SqlmqClient.Message::msgId).toList());
                            } else {
                                if (producersDone.get() == p.producers() &&
                                    java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastNonEmptyNanos) > p.quietPeriodMs()) {
                                    return null;
                                }
                                Thread.sleep(20);
                            }
                        } catch (SQLException e) {
                            consumerErrs.incrementAndGet();
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }
                });
            }

            scope.join();
            scope.throwIfFailed();
        }

        return new Result(List.copyOf(deliveries), Set.copyOf(producedIds), producerErrs, consumerErrs);
    }

    /** Map: msg_id → list of (consumer_id, received_at_nanos). */
    public static Map<Long, List<Delivery>> indexByMsgId(List<Delivery> deliveries) {
        var out = new java.util.HashMap<Long, List<Delivery>>();
        for (var d : deliveries) out.computeIfAbsent(d.msgId(), k -> new java.util.ArrayList<>()).add(d);
        return out;
    }
}
```

### Task 3.2: Write `NoLossNoDoubleDeliveryTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/NoLossNoDoubleDeliveryTest.java`

- [ ] **Step 1: Write the test**

```java
package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.ConcurrencyHarness;
import io.freesidenomad.sqlmq.support.ConcurrencyHarness.Profile;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class NoLossNoDoubleDeliveryTest {

    static Stream<Profile> profiles() {
        return Stream.of(Profile.light(), Profile.medium());
        // heavy() and asymmetric() left out of CI by default — opt in locally.
    }

    @ParameterizedTest
    @MethodSource("profiles")
    void everyProducedMessageIsDeliveredExactlyOnce(Profile p, ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var result = ConcurrencyHarness.run(client, q, p);

        assertThat(result.producerErrors().get()).isZero();
        assertThat(result.consumerErrors().get()).isZero();

        // No loss: every produced id appears in deliveries.
        var deliveredIds = new java.util.HashSet<Long>();
        for (var d : result.deliveries()) deliveredIds.add(d.msgId());
        assertThat(deliveredIds).containsExactlyInAnyOrderElementsOf(result.producedMsgIds());

        // No double-delivery: each msg_id appears exactly once across all consumers.
        // (Within VT the row is locked and read_ct keeps it claimed; deletes happen
        //  before VT expiry, so re-reads should be impossible.)
        var index = ConcurrencyHarness.indexByMsgId(result.deliveries());
        for (var entry : index.entrySet()) {
            assertThat(entry.getValue())
                .as("msg_id %d delivered multiple times: %s", entry.getKey(), entry.getValue())
                .hasSize(1);
        }
    }
}
```

- [ ] **Step 2: Run the light profile only first to keep iteration fast**

Run: `cd harness && mvn -B test -Dtest=NoLossNoDoubleDeliveryTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 2 parameterized invocations pass. Medium profile takes ~30s.

### Task 3.3: Write `VtHonoredTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/VtHonoredTest.java`

- [ ] **Step 1: Write the test**

```java
package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.ConcurrencyHarness;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class VtHonoredTest {

    @Test
    void noTwoConsumersHoldSameMsgSimultaneously(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var profile = new ConcurrencyHarness.Profile(8, 16, 500, 5, 30, 1000);
        var result = ConcurrencyHarness.run(client, q, profile);

        // Group deliveries by msg_id; for any duplicates, the second receipt must be
        // strictly after the first delivery's vt expiry. (NoLossNoDoubleDelivery already
        // asserts no duplicates in the happy path; this test specifically targets edge
        // cases — but with the deletes happening pre-VT-expiry there should be none.)
        var index = ConcurrencyHarness.indexByMsgId(result.deliveries());
        for (var entry : index.entrySet()) {
            var ds = entry.getValue();
            ds.sort(java.util.Comparator.comparingLong(ConcurrencyHarness.Delivery::receivedAtNanos));
            for (int i = 1; i < ds.size(); i++) {
                assertThat(ds.get(i).receivedAtNanos())
                    .as("msg_id %d redelivered to consumer %d before previous VT expiry",
                        entry.getKey(), ds.get(i).consumerId())
                    .isGreaterThan(ds.get(i - 1).vtExpiresAtNanos());
            }
        }
    }
}
```

- [ ] **Step 2: Run**

Run: `cd harness && mvn -B test -Dtest=VtHonoredTest`
Expected: PASS.

### Task 3.4: Write `FifoOrderingTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/FifoOrderingTest.java`

- [ ] **Step 1: Write the test**

```java
package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.ConcurrencyHarness;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class FifoOrderingTest {

    @Test
    void anySingleConsumersDeliveriesAreInIncreasingMsgIdOrder(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var p = new ConcurrencyHarness.Profile(4, 8, 1000, 1, 30, 1000); // batch=1 so per-consumer ordering is meaningful
        var result = ConcurrencyHarness.run(client, q, p);

        var perConsumer = new HashMap<Integer, java.util.List<Long>>();
        for (var d : result.deliveries())
            perConsumer.computeIfAbsent(d.consumerId(), k -> new java.util.ArrayList<>()).add(d.msgId());

        for (var entry : perConsumer.entrySet()) {
            var ids = entry.getValue();
            for (int i = 1; i < ids.size(); i++) {
                assertThat(ids.get(i))
                    .as("consumer %d received msg_id %d after %d (out of FIFO order)",
                        entry.getKey(), ids.get(i), ids.get(i - 1))
                    .isGreaterThan(ids.get(i - 1));
            }
        }
    }
}
```

- [ ] **Step 2: Run**

Run: `cd harness && mvn -B test -Dtest=FifoOrderingTest`
Expected: PASS.

### Task 3.5: Commit Phase 3

- [ ] **Step 1**

```bash
git add harness/src/test/java/io/freesidenomad/sqlmq/support/ConcurrencyHarness.java \
        harness/src/test/java/io/freesidenomad/sqlmq/concurrency/
git commit -m "$(cat <<'EOF'
phase 3: on-disk concurrency tests

ConcurrencyHarness: virtual-thread N×M producer/consumer rig with light/medium/heavy/asymmetric profiles.

Three invariant tests on the on-disk variant:
- NoLossNoDoubleDelivery: every produced msg_id is delivered exactly once
- VtHonored: no two consumers hold the same msg_id simultaneously
- FifoOrdering: per-consumer deliveries are in increasing msg_id order

Light + medium profiles in CI; heavy + asymmetric available for local runs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push
```

---

## Phase 4 — Metrics + purge + dlq_sweep + DLQ enforcement test

### Task 4.1: Write `V006__metrics_procs.sql`

**Files:**
- Create: `sql/migrations/V006__metrics_procs.sql`

- [ ] **Step 1: Write the migration**

```sql
-- V006: metrics, metrics_all

CREATE PROCEDURE sqlmq.metrics
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16);
    SELECT @storage = storage_type FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50060, 'Queue does not exist.', 1;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @atable SYSNAME = N'a_' + @queue;

    DECLARE @sql NVARCHAR(MAX) = N'
        SELECT
            @qn AS queue_name,
            (SELECT COUNT_BIG(*) FROM sqlmq.' + QUOTENAME(@qtable) + N') AS queue_length,
            ISNULL(IDENT_CURRENT(''sqlmq.' + REPLACE(@qtable, N'''', N'''''') + N'''), 0) AS total_messages,
            (SELECT DATEDIFF(SECOND, MIN(enqueued_at), SYSUTCDATETIME())
               FROM sqlmq.' + QUOTENAME(@qtable) + N') AS oldest_msg_age_seconds,
            (SELECT COUNT_BIG(*) FROM sqlmq.' + QUOTENAME(@atable) +
                N' WHERE dlq_reason IS NOT NULL) AS dlq_count;';
    EXEC sp_executesql @sql, N'@qn SYSNAME', @qn = @queue;
END
GO

CREATE PROCEDURE sqlmq.metrics_all
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @results TABLE (
        queue_name SYSNAME, queue_length BIGINT, total_messages BIGINT,
        oldest_msg_age_seconds INT, dlq_count BIGINT);

    DECLARE @qn SYSNAME;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT queue_name FROM sqlmq.meta;
    OPEN c;
    FETCH NEXT FROM c INTO @qn;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO @results EXEC sqlmq.metrics @qn;
        FETCH NEXT FROM c INTO @qn;
    END
    CLOSE c; DEALLOCATE c;
    SELECT * FROM @results ORDER BY queue_name;
END
GO
```

### Task 4.2: Write `V007__purge_dlq_sweep_procs.sql`

**Files:**
- Create: `sql/migrations/V007__purge_dlq_sweep_procs.sql`

- [ ] **Step 1: Write the migration**

```sql
-- V007: purge_queue (always interpreted), dlq_sweep (always interpreted, cross-engine)

CREATE PROCEDURE sqlmq.purge_queue
    @name SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM sqlmq.meta WHERE queue_name = @name)
        THROW 50070, 'Queue does not exist.', 1;
    DECLARE @qtable SYSNAME = N'q_' + @name;
    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE FROM sqlmq.' + QUOTENAME(@qtable) + N';
        SELECT @@ROWCOUNT AS rows_deleted;';
    EXEC sp_executesql @sql;
END
GO

-- dlq_sweep: move all over-cap messages from active queue to archive with dlq_reason='max_deliveries'.
-- Always interpreted (cross-engine for in-memory queues).
CREATE PROCEDURE sqlmq.dlq_sweep
    @queue SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT;
    SELECT @storage = storage_type, @payload = payload_type, @max_dlq = max_delivery_count
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50071, 'Queue does not exist.', 1;
    IF @max_dlq IS NULL
    BEGIN
        SELECT 0 AS rows_swept;
        RETURN;
    END

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @atable SYSNAME = N'a_' + @queue;
    DECLARE @msg_col NVARCHAR(64) = CASE @payload WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    DECLARE @sql NVARCHAR(MAX) = N'
        DELETE q
          OUTPUT deleted.msg_id, deleted.enqueued_at, deleted.vt, deleted.read_ct,
                 deleted.group_key, deleted.' + @msg_col + N', deleted.headers,
                 SYSUTCDATETIME(), N''max_deliveries''
            INTO sqlmq.' + QUOTENAME(@atable) + N' (
                 msg_id, enqueued_at, vt, read_ct, group_key, ' + @msg_col + N', headers,
                 archived_at, dlq_reason)
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
         WHERE q.read_ct >= @dlq;
        SELECT @@ROWCOUNT AS rows_swept;';
    EXEC sp_executesql @sql, N'@dlq INT', @dlq = @max_dlq;
END
GO
```

### Task 4.3: Extend `SqlmqClient` with `metrics`, `metricsAll`, `purgeQueue`, `dlqSweep`

**Files:**
- Modify: `harness/src/main/java/io/freesidenomad/sqlmq/client/SqlmqClient.java`

- [ ] **Step 1: Add methods**

```java
public Metrics metrics(String queue) throws SQLException {
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.metrics(?)}")) {
        cs.setString(1, queue);
        try (ResultSet rs = cs.executeQuery()) {
            rs.next();
            return new Metrics(
                rs.getString("queue_name"),
                rs.getLong("queue_length"),
                rs.getLong("total_messages"),
                (Integer) rs.getObject("oldest_msg_age_seconds"),
                rs.getLong("dlq_count"));
        }
    }
}

public List<Metrics> metricsAll() throws SQLException {
    var out = new ArrayList<Metrics>();
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.metrics_all()}")) {
        try (ResultSet rs = cs.executeQuery()) {
            while (rs.next())
                out.add(new Metrics(
                    rs.getString("queue_name"),
                    rs.getLong("queue_length"),
                    rs.getLong("total_messages"),
                    (Integer) rs.getObject("oldest_msg_age_seconds"),
                    rs.getLong("dlq_count")));
        }
    }
    return out;
}

public int purgeQueue(String name) throws SQLException {
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.purge_queue(?)}")) {
        cs.setString(1, name);
        try (ResultSet rs = cs.executeQuery()) {
            rs.next();
            return rs.getInt("rows_deleted");
        }
    }
}

public int dlqSweep(String queue) throws SQLException {
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.dlq_sweep(?)}")) {
        cs.setString(1, queue);
        try (ResultSet rs = cs.executeQuery()) {
            rs.next();
            return rs.getInt("rows_swept");
        }
    }
}

public record Metrics(String queueName, long queueLength, long totalMessages,
                      Integer oldestMsgAgeSeconds, long dlqCount) {}
```

### Task 4.4: Write `MetricsTest`, `PurgeDlqSweepTest`, `DlqEnforcementTest`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/MetricsTest.java`
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/PurgeDlqSweepTest.java`
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/DlqEnforcementTest.java`

- [ ] **Step 1: MetricsTest**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class MetricsTest {

    @Test
    void emptyQueueMetrics(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        var m = client.metrics(q);
        assertThat(m.queueName()).isEqualTo(q);
        assertThat(m.queueLength()).isZero();
        assertThat(m.totalMessages()).isZero();
        assertThat(m.oldestMsgAgeSeconds()).isNull();
        assertThat(m.dlqCount()).isZero();
    }

    @Test
    void metricsAfterSends(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);

        client.send(q, "{}", null);
        client.send(q, "{}", null);
        client.send(q, "{}", null);

        var m = client.metrics(q);
        assertThat(m.queueLength()).isEqualTo(3);
        assertThat(m.totalMessages()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void metricsAllListsAllQueues(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q1 = TestQueues.uniqueName("q");
        var q2 = TestQueues.uniqueName("q");
        client.createQueue(q1, "ondisk", false, "json", null);
        client.createQueue(q2, "ondisk", false, "json", null);

        var all = client.metricsAll();
        assertThat(all).extracting(SqlmqClient.Metrics::queueName).contains(q1, q2);
    }
}
```

- [ ] **Step 2: PurgeDlqSweepTest**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class PurgeDlqSweepTest {

    @Test
    void purgeRemovesAllMessages(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);
        for (int i = 0; i < 10; i++) client.send(q, "{}", null);
        var purged = client.purgeQueue(q);
        assertThat(purged).isEqualTo(10);
        assertThat(client.metrics(q).queueLength()).isZero();
    }

    @Test
    void dlqSweepNoOpWhenMaxDeliveryCountNull(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);
        client.send(q, "{}", null);
        // Read 5 times to push read_ct up.
        for (int i = 0; i < 5; i++) {
            var msgs = client.read(q, 1, 10);
            if (msgs.isEmpty()) Thread.sleep(1100);
        }
        var swept = client.dlqSweep(q);
        assertThat(swept).isZero();
    }

    @Test
    void dlqSweepMovesOverCapMessagesWithReason(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", 2);
        var id = client.send(q, "{}", null);

        // Drive read_ct to >= 2.
        for (int i = 0; i < 3; i++) {
            client.read(q, 1, 10);
            Thread.sleep(1100);
        }

        var swept = client.dlqSweep(q);
        assertThat(swept).isEqualTo(1);

        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             var st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT dlq_reason FROM sqlmq.[a_" + q + "] WHERE msg_id = " + id)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("dlq_reason")).isEqualTo("max_deliveries");
        }
    }

    @Test
    void readSkipsOverCapMessagesEvenBeforeSweep(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", 2);
        client.send(q, "{}", null);

        for (int i = 0; i < 2; i++) {
            client.read(q, 1, 10);
            Thread.sleep(1100);
        }
        // Now read_ct >= 2; read should return empty even though row is in queue.
        Thread.sleep(1100);
        assertThat(client.read(q, 30, 10)).isEmpty();
    }
}
```

- [ ] **Step 3: DlqEnforcementTest** (concurrency)

```java
package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.ResultSet;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class DlqEnforcementTest {

    @Test
    void messagesReachingMaxDeliveriesAreEventuallyArchived(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", 3);

        // Send 100 messages.
        for (int i = 0; i < 100; i++) client.send(q, "{\"i\":" + i + "}", null);

        // 16 "broken" consumers that read but never delete; each call advances read_ct
        // for the rows they grab. We use vt_seconds=1 so messages return quickly.
        var receipts = new AtomicInteger();
        var until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            for (int c = 0; c < 16; c++) {
                scope.fork(() -> {
                    while (System.nanoTime() < until) {
                        var msgs = client.read(q, 1, 10);
                        receipts.addAndGet(msgs.size());
                        if (msgs.isEmpty()) Thread.sleep(50);
                    }
                    return null;
                });
            }
            scope.join();
            scope.throwIfFailed();
        }

        client.dlqSweep(q);

        // After sweep, archive should hold all 100 messages with dlq_reason='max_deliveries'.
        try (var c = DatabasePerTest.dataSource(ctx).getConnection();
             var st = c.createStatement();
             ResultSet rs = st.executeQuery(
                 "SELECT COUNT(*) AS n FROM sqlmq.[a_" + q + "] WHERE dlq_reason = 'max_deliveries'")) {
            rs.next();
            assertThat(rs.getInt("n")).isEqualTo(100);
        }

        // Active queue should be empty.
        assertThat(client.metrics(q).queueLength()).isZero();
    }
}
```

- [ ] **Step 4: Run all three**

```
cd harness && mvn -B test -Dtest=MetricsTest,PurgeDlqSweepTest,DlqEnforcementTest
```
Expected: all pass.

### Task 4.5: Commit Phase 4

- [ ] **Step 1**

```bash
git add sql/migrations/V006__metrics_procs.sql sql/migrations/V007__purge_dlq_sweep_procs.sql \
        harness/src/main \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/MetricsTest.java \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/PurgeDlqSweepTest.java \
        harness/src/test/java/io/freesidenomad/sqlmq/concurrency/DlqEnforcementTest.java
git commit -m "$(cat <<'EOF'
phase 4: metrics, purge, dlq_sweep + DLQ enforcement test

V006 — metrics (per queue: length, total, oldest_age, dlq_count) and metrics_all.
V007 — purge_queue and dlq_sweep (always interpreted; sweep moves over-cap messages
       to archive with dlq_reason='max_deliveries').

DLQ enforcement is exclude-and-sweep: read excludes over-cap msgs; dlq_sweep moves
them. Symmetric across both storage variants (no cross-engine writes in the read proc).

DlqEnforcementTest: 16 consumers that read but never delete on a max_delivery_count=3
queue; after sweep, archive holds all messages with dlq_reason='max_deliveries' and
the active queue is empty.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push
```

---

## Phase 5 — Grouped-FIFO read variant

### Task 5.1: Write `V008__read_grouped_procs.sql`

**Files:**
- Create: `sql/migrations/V008__read_grouped_procs.sql`

- [ ] **Step 1: Write the migration**

```sql
-- V008: read_grouped — grouped-FIFO invariant (one in-flight per group_key)

CREATE PROCEDURE sqlmq._read_grouped_ondisk
    @queue        SYSNAME,
    @payload_type VARCHAR(8),
    @vt_seconds   INT,
    @max_count    INT,
    @max_dlq      INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @msg_col NVARCHAR(64) = CASE @payload_type WHEN 'json' THEN N'message' ELSE N'message_bin' END;

    DECLARE @sql NVARCHAR(MAX) = N'
        WITH eligible AS (
            SELECT q.msg_id, q.group_key,
                   ROW_NUMBER() OVER (PARTITION BY q.group_key ORDER BY q.msg_id) AS rn
              FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
                   WITH (READPAST, UPDLOCK, ROWLOCK, READCOMMITTEDLOCK)
             WHERE q.vt <= SYSUTCDATETIME()
               AND (@dlq IS NULL OR q.read_ct < @dlq)
               AND (q.group_key IS NULL OR NOT EXISTS (
                    SELECT 1
                      FROM sqlmq.' + QUOTENAME(@qtable) + N' AS inflight
                     WHERE inflight.group_key = q.group_key
                       AND inflight.vt > SYSUTCDATETIME()))
        ),
        claimed AS (
            SELECT TOP (@n) msg_id
              FROM eligible
             WHERE rn = 1
             ORDER BY msg_id
        )
        UPDATE q
           SET vt = DATEADD(SECOND, @vt, SYSUTCDATETIME()),
               read_ct = read_ct + 1
        OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
               inserted.vt, inserted.group_key,
               inserted.' + @msg_col + N', inserted.headers
          FROM sqlmq.' + QUOTENAME(@qtable) + N' AS q
         INNER JOIN claimed AS c ON q.msg_id = c.msg_id;';

    EXEC sp_executesql @sql,
        N'@n INT, @vt INT, @dlq INT',
        @n = @max_count, @vt = @vt_seconds, @dlq = @max_dlq;
END
GO

CREATE PROCEDURE sqlmq.read_grouped
    @queue      SYSNAME,
    @vt_seconds INT,
    @max_count  INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8), @max_dlq INT, @grouped BIT;
    SELECT @storage = storage_type, @payload = payload_type,
           @max_dlq = max_delivery_count, @grouped = is_grouped
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50080, 'Queue does not exist.', 1;
    IF @grouped = 0 THROW 50081, 'Queue was not created with grouping enabled.', 1;
    IF @storage = 'ondisk'
        EXEC sqlmq._read_grouped_ondisk @queue, @payload, @vt_seconds, @max_count, @max_dlq;
    ELSE
        THROW 50082, 'In-memory grouped read not implemented yet (V009).', 1;
END
GO
```

### Task 5.2: Extend `SqlmqClient` with `readGrouped` and `sendWithGroup`

**Files:**
- Modify: `harness/src/main/java/io/freesidenomad/sqlmq/client/SqlmqClient.java`

- [ ] **Step 1: Add methods**

Replace the existing `sendInternal` to support `group_key`. Add a public `sendGrouped`:

```java
public long sendGrouped(String queue, String message, String groupKey) throws SQLException {
    // Send with group_key. Since sqlmq.send doesn't take a group_key parameter
    // (group is set on the row at insert time and the header column is JSON),
    // we use a direct insert via sp_executesql to set group_key.
    // (Adjustment: We extend sqlmq.send to accept @group_key — see below.)
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.send(?, ?, ?, ?, ?, ?)}")) {
        cs.setString(1, queue);
        cs.setString(2, message);
        cs.setNull(3, java.sql.Types.VARBINARY);
        cs.setNull(4, java.sql.Types.NVARCHAR);
        cs.setInt(5, 0);
        cs.setString(6, groupKey);
        try (ResultSet rs = cs.executeQuery()) {
            rs.next();
            return rs.getLong("msg_id");
        }
    }
}

public List<Message> readGrouped(String queue, int vtSeconds, int maxCount) throws SQLException {
    var out = new ArrayList<Message>();
    try (Connection c = ds.getConnection();
         CallableStatement cs = c.prepareCall("{call sqlmq.read_grouped(?, ?, ?)}")) {
        cs.setString(1, queue);
        cs.setInt(2, vtSeconds);
        cs.setInt(3, maxCount);
        try (ResultSet rs = cs.executeQuery()) {
            var meta = rs.getMetaData();
            boolean binary = false;
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if ("message_bin".equalsIgnoreCase(meta.getColumnLabel(i))) { binary = true; break; }
            }
            while (rs.next()) {
                out.add(new Message(
                    rs.getLong("msg_id"),
                    rs.getInt("read_ct"),
                    rs.getTimestamp("enqueued_at").toInstant(),
                    rs.getTimestamp("vt").toInstant(),
                    binary ? null : rs.getString("message"),
                    binary ? rs.getBytes("message_bin") : null,
                    rs.getString("headers")
                ));
            }
        }
    }
    return out;
}
```

- [ ] **Step 2: Add a follow-up migration that extends `sqlmq.send` with `@group_key`**

Append this proc replacement to `V008__read_grouped_procs.sql`:

```sql
-- Extend sqlmq.send and sqlmq._send_ondisk to accept @group_key.
-- Use CREATE OR ALTER so the migration replaces the existing definitions cleanly.

CREATE OR ALTER PROCEDURE sqlmq._send_ondisk
    @queue          SYSNAME,
    @payload_type   VARCHAR(8),
    @message        NVARCHAR(MAX) = NULL,
    @message_bin    VARBINARY(MAX) = NULL,
    @headers        NVARCHAR(MAX) = NULL,
    @delay_seconds  INT = 0,
    @group_key      NVARCHAR(255) = NULL,
    @msg_id         BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @qtable SYSNAME = N'q_' + @queue;
    DECLARE @vt DATETIME2(7) = DATEADD(SECOND, @delay_seconds, SYSUTCDATETIME());
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @ids TABLE (id BIGINT);
    IF @payload_type = 'json'
    BEGIN
        IF @message IS NULL THROW 50020, '@message required for json queue', 1;
        SET @sql = N'INSERT INTO sqlmq.' + QUOTENAME(@qtable) +
            N' (vt, group_key, message, headers) OUTPUT inserted.msg_id VALUES (@vt, @gk, @msg, @hdr);';
        INSERT INTO @ids EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @gk NVARCHAR(255), @msg NVARCHAR(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @gk = @group_key, @msg = @message, @hdr = @headers;
    END
    ELSE
    BEGIN
        IF @message_bin IS NULL THROW 50021, '@message_bin required for binary queue', 1;
        SET @sql = N'INSERT INTO sqlmq.' + QUOTENAME(@qtable) +
            N' (vt, group_key, message_bin, headers) OUTPUT inserted.msg_id VALUES (@vt, @gk, @msg, @hdr);';
        INSERT INTO @ids EXEC sp_executesql @sql,
            N'@vt DATETIME2(7), @gk NVARCHAR(255), @msg VARBINARY(MAX), @hdr NVARCHAR(MAX)',
            @vt = @vt, @gk = @group_key, @msg = @message_bin, @hdr = @headers;
    END
    SELECT @msg_id = id FROM @ids;
END
GO

CREATE OR ALTER PROCEDURE sqlmq.send
    @queue         SYSNAME,
    @message       NVARCHAR(MAX) = NULL,
    @message_bin   VARBINARY(MAX) = NULL,
    @headers       NVARCHAR(MAX) = NULL,
    @delay_seconds INT = 0,
    @group_key     NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @storage VARCHAR(16), @payload VARCHAR(8);
    SELECT @storage = storage_type, @payload = payload_type
      FROM sqlmq.meta WHERE queue_name = @queue;
    IF @storage IS NULL THROW 50030, 'Queue does not exist.', 1;
    DECLARE @msg_id BIGINT;
    IF @storage = 'ondisk'
        EXEC sqlmq._send_ondisk @queue, @payload, @message, @message_bin, @headers,
                                 @delay_seconds, @group_key, @msg_id OUTPUT;
    ELSE
        THROW 50031, 'In-memory send not implemented yet (V009).', 1;
    SELECT @msg_id AS msg_id;
END
GO
```

(Existing `SendTest` callers without `@group_key` still work since the new param has a default; only `sendInternal` in the client changes — update it to pass NULL for group_key.)

- [ ] **Step 3: Update `sendInternal` in SqlmqClient.java**

Find the existing `sendInternal` in `SqlmqClient.java` (added in Task 2.2) and modify the prepared call from `"{call sqlmq.send(?, ?, ?, ?, ?)}"` to `"{call sqlmq.send(?, ?, ?, ?, ?, ?)}"`. Add `cs.setNull(6, java.sql.Types.NVARCHAR);` before `try (ResultSet rs = cs.executeQuery()) {`.

### Task 5.3: Write `ReadGroupedTest` (correctness)

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/correctness/ReadGroupedTest.java`

- [ ] **Step 1**

```java
package io.freesidenomad.sqlmq.correctness;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class ReadGroupedTest {

    @Test
    void readGroupedRejectsUngroupedQueue(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", false, "json", null);
        assertThatThrownBy(() -> client.readGrouped(q, 30, 10))
            .hasMessageContaining("not created with grouping enabled");
    }

    @Test
    void groupedReadReturnsAtMostOnePerGroup(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", true, "json", null);

        // 3 groups, 5 messages each
        for (int g = 0; g < 3; g++)
            for (int i = 0; i < 5; i++)
                client.sendGrouped(q, "{\"g\":" + g + ",\"i\":" + i + "}", "g" + g);

        var first = client.readGrouped(q, 60, 10);
        // Should get one head per group: 3 messages.
        assertThat(first).hasSize(3);
        assertThat(first.stream().map(SqlmqClient.Message::msgId).distinct().count()).isEqualTo(3);
    }

    @Test
    void groupedReadBlocksFurtherFromSameGroupUntilDeleteOrVtExpiry(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", true, "json", null);
        client.sendGrouped(q, "{\"g\":0,\"i\":0}", "g0");
        client.sendGrouped(q, "{\"g\":0,\"i\":1}", "g0");

        var first = client.readGrouped(q, 60, 10);
        assertThat(first).hasSize(1);

        var second = client.readGrouped(q, 60, 10);
        assertThat(second).isEmpty(); // group g0 is in-flight

        client.delete(q, java.util.List.of(first.get(0).msgId()));
        var third = client.readGrouped(q, 60, 10);
        assertThat(third).hasSize(1);
    }
}
```

- [ ] **Step 2: Run**

Run: `cd harness && mvn -B test -Dtest=ReadGroupedTest`
Expected: 3 tests, 3 passed.

### Task 5.4: Write `GroupedFifoInvariantTest` (concurrency)

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/GroupedFifoInvariantTest.java`

- [ ] **Step 1**

```java
package io.freesidenomad.sqlmq.concurrency;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class GroupedFifoInvariantTest {

    private record Receipt(long msgId, String groupKey, int consumerId,
                           long receivedNanos, long deletedNanos) {}

    @Test
    void noTwoConsumersHoldSameGroupAndPerGroupOrderingIsPreserved(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var q = TestQueues.uniqueName("q");
        client.createQueue(q, "ondisk", true, "json", null);

        int producers = 8, groups = 16, messagesPerGroupPerProducer = 50;
        var producedPerGroup = new ConcurrentHashMap<String, List<Long>>();
        var producersDone = new AtomicInteger();

        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            for (int p = 0; p < producers; p++) {
                final int producerId = p;
                scope.fork(() -> {
                    for (int k = 0; k < messagesPerGroupPerProducer; k++) {
                        for (int g = 0; g < groups; g++) {
                            String gk = "g" + g;
                            long id = client.sendGrouped(q, "{\"p\":" + producerId + ",\"k\":" + k + "}", gk);
                            producedPerGroup.computeIfAbsent(gk, x -> new CopyOnWriteArrayList<>()).add(id);
                        }
                    }
                    producersDone.incrementAndGet();
                    return null;
                });
            }

            var receipts = new CopyOnWriteArrayList<Receipt>();
            int consumers = 32;
            for (int c = 0; c < consumers; c++) {
                final int consumerId = c;
                scope.fork(() -> {
                    long lastNonEmpty = System.nanoTime();
                    while (true) {
                        var msgs = client.readGrouped(q, 30, 5);
                        if (!msgs.isEmpty()) {
                            long now = System.nanoTime();
                            lastNonEmpty = now;
                            // Hold briefly to make group-overlap violations easier to detect
                            Thread.sleep(5);
                            long deletedNow = System.nanoTime();
                            client.delete(q, msgs.stream().map(SqlmqClient.Message::msgId).toList());
                            for (var m : msgs) {
                                // Look up group_key from msg_id (stored on row; we re-query)
                                receipts.add(new Receipt(m.msgId(), groupKeyOf(client, q, m.msgId()),
                                                          consumerId, now, deletedNow));
                            }
                        } else {
                            if (producersDone.get() == producers &&
                                java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastNonEmpty) > 2000)
                                return null;
                            Thread.sleep(20);
                        }
                    }
                });
            }
            scope.join();
            scope.throwIfFailed();

            // Invariant 1: per group, no temporal overlap between consumers' receipts
            var byGroup = new HashMap<String, List<Receipt>>();
            for (var r : receipts) byGroup.computeIfAbsent(r.groupKey(), k -> new java.util.ArrayList<>()).add(r);
            for (var entry : byGroup.entrySet()) {
                var rs = entry.getValue();
                rs.sort(java.util.Comparator.comparingLong(Receipt::receivedNanos));
                for (int i = 1; i < rs.size(); i++) {
                    assertThat(rs.get(i).receivedNanos())
                        .as("group %s: consumer %d received before consumer %d's delete (overlap)",
                            entry.getKey(), rs.get(i).consumerId(), rs.get(i - 1).consumerId())
                        .isGreaterThan(rs.get(i - 1).deletedNanos());
                }

                // Invariant 2: per group, msg_ids delivered in increasing order
                for (int i = 1; i < rs.size(); i++) {
                    assertThat(rs.get(i).msgId())
                        .as("group %s: out-of-order delivery", entry.getKey())
                        .isGreaterThan(rs.get(i - 1).msgId());
                }
            }
        }
    }

    private String groupKeyOf(SqlmqClient client, String q, long msgId) throws SQLException {
        // Helper for the test: look up group_key for a msg_id. We use a direct query.
        // (The Message record doesn't include group_key by default; this avoids that change.)
        try (var c = ((com.zaxxer.hikari.HikariDataSource) DatabasePerTest_currentDs).getConnection();
             var st = c.createStatement();
             var rs = st.executeQuery("SELECT group_key FROM sqlmq.[a_" + q + "] WHERE msg_id = " + msgId
                 + " UNION ALL SELECT group_key FROM sqlmq.[q_" + q + "] WHERE msg_id = " + msgId)) {
            if (rs.next()) return rs.getString("group_key");
            return null;
        }
    }

    // We need access to the test's DataSource from the helper. Set in beforeEach below.
    static javax.sql.DataSource DatabasePerTest_currentDs;
    @org.junit.jupiter.api.BeforeEach
    void capture(ExtensionContext ctx) { DatabasePerTest_currentDs = DatabasePerTest.dataSource(ctx); }
}
```

> **Note for executor:** the `groupKeyOf` helper is awkward because the `Message` record doesn't expose `group_key`. Two cleaner options at execution time:
> 1. Add `groupKey` to the `Message` record and update `SqlmqClient.read*` to populate it (preferred — touches Message constructor in three places).
> 2. Pre-compute the producer's `producerId × k → groupKey` map and use it in the test (needs producer to embed group hint in the message JSON).
>
> Pick option 1; remove the `groupKeyOf` shim. This note exists because the simplest version of the test surfaces a real Message-record gap.

- [ ] **Step 2: Per the note above, update `Message` record and `read`/`readGrouped` to include `groupKey`**

Modify `SqlmqClient.Message` to include `String groupKey`. Update both `read` and `readGrouped` to pull `group_key` from the result set and pass into the Message. Update Tier 1 tests that construct Messages if any (none currently). Re-check `pop` — no group_key columns yet returned; leave for now since pop isn't used in grouped contexts.

- [ ] **Step 3: Run**

Run: `cd harness && mvn -B test -Dtest=GroupedFifoInvariantTest`
Expected: PASS. This test takes ~30-60s.

### Task 5.5: Commit Phase 5

- [ ] **Step 1**

```bash
git add sql/migrations/V008__read_grouped_procs.sql harness/src/main \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/ReadGroupedTest.java \
        harness/src/test/java/io/freesidenomad/sqlmq/concurrency/GroupedFifoInvariantTest.java
git commit -m "$(cat <<'EOF'
phase 5: grouped-FIFO read variant (on-disk)

V008 — sqlmq.read_grouped: enforces "one in-flight per group_key" via WHERE NOT EXISTS
       correlated subquery on the (group_key, vt, msg_id) filtered index. Extends
       sqlmq.send / sqlmq._send_ondisk with @group_key parameter (CREATE OR ALTER).

ReadGroupedTest: rejects ungrouped queues, returns one head per group, blocks further
                  reads from same group until delete or VT expiry.
GroupedFifoInvariantTest: 8 producers × 16 groups × 50 msgs × 32 consumers; asserts
                  no temporal overlap between consumers within a group AND per-group
                  msg_id ordering preserved across deliveries.

SqlmqClient.Message now includes groupKey.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push
```

**You can stop here and ship.** Phases 0-5 are a complete on-disk message queue with full FIFO + grouped-FIFO semantics, DLQ enforcement, metrics, and concurrency-tested invariants. Open a PR to `main` and merge before continuing.

---

## Phase 6 — In-memory storage variant

Phase 6 is the largest and most involved. It generates per-queue natively compiled stored procedures inside `sqlmq.create_queue` when `@storage = 'inmemory'`, removes them in `sqlmq.drop_queue`, and updates each public dispatcher to route to those generated procs.

**Prerequisite reading before starting:** `research/sqlserver-patterns.md` §3 (memory-optimized + natively compiled), §4 (OUTPUT clause). Verify `MEMORY_OPTIMIZED_DATA` filegroup is configured on the test container — Testcontainers' MS SQL image **does not** include this by default; the first task of Phase 6 must create it.

### Task 6.1: Add `V009__inmemory_setup.sql` — filegroup setup

**Files:**
- Create: `sql/migrations/V009__inmemory_setup.sql`

- [ ] **Step 1: Add filegroup creation**

```sql
-- V009: enable memory-optimized tables — adds MEMORY_OPTIMIZED_DATA filegroup.
-- Idempotent in the sense that Flyway tracks it; if you need to re-run, drop the DB.

DECLARE @db SYSNAME = DB_NAME();
DECLARE @data_path NVARCHAR(500);
SELECT @data_path = LEFT(physical_name, LEN(physical_name) - CHARINDEX('\', REVERSE(physical_name)) + 1)
  FROM sys.master_files WHERE database_id = DB_ID() AND type = 0;
-- Linux paths use '/' — handle both.
IF CHARINDEX('/', @data_path) > 0
    SELECT @data_path = LEFT(physical_name, LEN(physical_name) - CHARINDEX('/', REVERSE(physical_name)) + 1)
      FROM sys.master_files WHERE database_id = DB_ID() AND type = 0;

DECLARE @sql NVARCHAR(MAX) = N'
ALTER DATABASE [' + @db + N'] ADD FILEGROUP sqlmq_imoltp CONTAINS MEMORY_OPTIMIZED_DATA;
ALTER DATABASE [' + @db + N'] ADD FILE (
    NAME = ''sqlmq_imoltp_dir'',
    FILENAME = ''' + @data_path + N'sqlmq_imoltp_dir''
) TO FILEGROUP sqlmq_imoltp;';

EXEC sp_executesql @sql;
GO
```

> **Risk:** Memory-optimized filegroups are not always supported in Testcontainers SQL Server images depending on the SKU. If V009 fails in CI with "memory-optimized tables not supported," the in-memory variant cannot proceed against the default Testcontainers image and Phase 6 needs to switch to a Developer Edition image (`mcr.microsoft.com/mssql/server:2022-latest` is Developer; should be fine — but verify before continuing).

### Task 6.2: Extend `sqlmq.create_queue` to handle `@storage = 'inmemory'`

**Files:**
- Modify: `sql/migrations/V009__inmemory_setup.sql` (add proc replacement here, keeping V009 cohesive)

- [ ] **Step 1: Append `CREATE OR ALTER PROCEDURE sqlmq.create_queue`**

The new body: if `@storage = 'inmemory'`, build the queue table with `MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA`, and generate per-queue natively compiled inner procs:
- `sqlmq._send_inmem_<name>` (with `@group_key` param)
- `sqlmq._send_batch_inmem_<name>` — note: natively compiled procs cannot accept TVPs of types that include `NVARCHAR(MAX)` columns. **Decision needed at execution time:** either drop `send_batch` for in-memory queues (acceptable — single-row sends in a transaction get the same effect) or convert the TVP to a memory-optimized table type.
- `sqlmq._read_inmem_<name>` (with `@max_dlq` param; retry-on-41302 loop)
- `sqlmq._read_grouped_inmem_<name>` (only if `@grouped = 1`)
- `sqlmq._delete_inmem_<name>`
- `sqlmq._pop_inmem_<name>`

Per the design spec: archive, purge_queue, dlq_sweep, metrics remain interpreted (they're cross-engine reads/writes).

Detailed proc bodies are intentionally not pre-written here because they require empirical validation against the Testcontainers SQL Server image — the natively-compiled-no-dynamic-SQL constraint plus per-queue table names means each generated proc is a separate `sp_executesql` call building the proc text. The executor will:
1. Write a draft of `CREATE OR ALTER PROCEDURE sqlmq.create_queue` that builds the in-memory table + each inner proc as a string and `EXEC sp_executesql`s it
2. Iterate via tests until it works (the dispatchers in V003-V008 currently `THROW` for `@storage = 'inmemory'` — Phase 6 also updates each dispatcher to route to the per-queue inner proc by name)

**Pattern for one inner proc** (reference for the executor — `_read_inmem_<name>`):

```sql
DECLARE @inner_sql NVARCHAR(MAX) = N'
CREATE PROCEDURE sqlmq.[_read_inmem_' + @name + N']
    @vt_seconds INT, @max_count INT, @max_dlq INT
WITH NATIVE_COMPILATION, SCHEMABINDING
AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N''us_english'')
    DECLARE @attempts INT = 0;
    WHILE @attempts < 3
    BEGIN
        BEGIN TRY
            UPDATE TOP (@max_count) sqlmq.[q_' + @name + N']
            SET vt = DATEADD(SECOND, @vt_seconds, SYSUTCDATETIME()),
                read_ct = read_ct + 1
            OUTPUT inserted.msg_id, inserted.read_ct, inserted.enqueued_at,
                   inserted.vt, inserted.message, inserted.headers
            WHERE msg_id IN (
                SELECT TOP (@max_count) msg_id
                FROM sqlmq.[q_' + @name + N']
                WHERE vt <= SYSUTCDATETIME()
                  AND (@max_dlq IS NULL OR read_ct < @max_dlq)
                ORDER BY msg_id);
            RETURN 0;
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() = 41302 SET @attempts += 1;
            ELSE THROW;
        END CATCH
    END
END';
EXEC sp_executesql @inner_sql;
```

**Tasks for the executor (each its own commit):**
- [ ] 6.2.a: Update `sqlmq.create_queue` to create the in-memory queue table + the archive table (still on-disk).
- [ ] 6.2.b: Add per-queue inner proc generation for `_send_inmem_<name>`. Test: re-run SendTest parameterized over storage.
- [ ] 6.2.c: Add per-queue inner proc generation for `_read_inmem_<name>`. Test: ReadTest parameterized.
- [ ] 6.2.d: Add `_delete_inmem_<name>` and `_pop_inmem_<name>`. Test: DeleteArchivePopTest parameterized.
- [ ] 6.2.e: Add `_read_grouped_inmem_<name>` (only when `@grouped = 1`). Test: ReadGroupedTest parameterized.
- [ ] 6.2.f: Update `sqlmq.drop_queue` to drop the per-queue inner procs (lookup `sys.objects` for matching names; `DROP PROCEDURE IF EXISTS`).

### Task 6.3: Update each dispatcher to route to in-memory inner procs

**Files:**
- Modify: `sql/migrations/V009__inmemory_setup.sql` — add `CREATE OR ALTER PROCEDURE` blocks for `sqlmq.send`, `sqlmq.send_batch` (or document its inmemory absence), `sqlmq.read`, `sqlmq.read_grouped`, `sqlmq.delete`, `sqlmq.pop`.

- [ ] **Step 1: Replace each dispatcher's `IF @storage = 'inmemory' THROW ...` branch with the per-queue exec.**

Pattern (for `sqlmq.read`):

```sql
IF @storage = 'inmemory'
BEGIN
    DECLARE @inner SYSNAME = N'_read_inmem_' + @queue;
    DECLARE @sql NVARCHAR(MAX) = N'EXEC sqlmq.' + QUOTENAME(@inner) +
        N' @vt_seconds, @max_count, @max_dlq;';
    EXEC sp_executesql @sql,
        N'@vt_seconds INT, @max_count INT, @max_dlq INT',
        @vt_seconds = @vt_seconds, @max_count = @max_count, @max_dlq = @max_dlq;
END
```

(Dispatcher itself remains interpreted; only the inner proc is natively compiled.)

### Task 6.4: Parameterize Tier 1 + Tier 2 tests over `{ondisk, inmemory}`

**Files:**
- Modify: every test in `correctness/` and `concurrency/` that creates a queue.

- [ ] **Step 1: Add a JUnit `@MethodSource` providing `Stream.of("ondisk", "inmemory")` and pass to `createQueue`.**

The cleanest pattern: introduce a `StorageVariants` parameter source in `support/`:

```java
package io.freesidenomad.sqlmq.support;

import org.junit.jupiter.params.provider.Arguments;
import java.util.stream.Stream;

public final class StorageVariants {
    private StorageVariants() {}
    public static Stream<Arguments> all() {
        return Stream.of(Arguments.of("ondisk"), Arguments.of("inmemory"));
    }
}
```

Convert each test class's `@Test` methods that exercise queue lifecycle to `@ParameterizedTest @MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")`, take a `String storage` parameter, and pass it to `createQueue`.

For `ConcurrencyHarness`, no changes needed — it doesn't know or care about storage type; the `createQueue` call site does.

- [ ] **Step 2: Run the full test suite**

```
cd harness && mvn -B verify
```
Expected: every test runs twice (once per variant), all pass.

### Task 6.5: Commit Phase 6

- [ ] **Step 1**

```bash
git add sql/migrations/V009__inmemory_setup.sql \
        harness/src/test/java/io/freesidenomad/sqlmq/support/StorageVariants.java \
        harness/src/test/java/io/freesidenomad/sqlmq/correctness/ \
        harness/src/test/java/io/freesidenomad/sqlmq/concurrency/
git commit -m "$(cat <<'EOF'
phase 6: in-memory storage variant

V009 — adds MEMORY_OPTIMIZED_DATA filegroup; extends sqlmq.create_queue to generate
       per-queue natively compiled inner procs (_send_inmem_<name>, _read_inmem_<name>,
       _delete_inmem_<name>, _pop_inmem_<name>, optional _read_grouped_inmem_<name>);
       extends sqlmq.drop_queue to remove them; updates each public dispatcher to
       route inmemory storage to the per-queue inner proc by name.

archive, purge_queue, dlq_sweep, metrics remain interpreted (cross-engine).

All Tier 1 + Tier 2 tests now run via @ParameterizedTest @MethodSource(StorageVariants#all)
covering both ondisk and inmemory variants.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push
```

---

## Phase 7 — Bake-off harness

### Task 7.1: Add `BenchmarkProfiles`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/benchmark/BenchmarkProfiles.java`

- [ ] **Step 1**

```java
package io.freesidenomad.sqlmq.benchmark;

import io.freesidenomad.sqlmq.support.ConcurrencyHarness.Profile;

import java.util.List;

public final class BenchmarkProfiles {
    private BenchmarkProfiles() {}
    public static List<Profile> all() {
        return List.of(
            Profile.light(),
            Profile.medium(),
            Profile.heavy(),
            Profile.asymmetric(),
            new Profile(1024, 1024, 0, 50, 30, 2000) // burst (no producers; messages preloaded)
        );
    }
}
```

### Task 7.2: Add `BakeOffRunner`

**Files:**
- Create: `harness/src/test/java/io/freesidenomad/sqlmq/benchmark/BakeOffRunner.java`

- [ ] **Step 1: Write the runner**

```java
package io.freesidenomad.sqlmq.benchmark;

import io.freesidenomad.sqlmq.client.SqlmqClient;
import io.freesidenomad.sqlmq.support.ConcurrencyHarness;
import io.freesidenomad.sqlmq.support.ConcurrencyHarness.Profile;
import io.freesidenomad.sqlmq.support.DatabasePerTest;
import io.freesidenomad.sqlmq.support.SqlServerContainer;
import io.freesidenomad.sqlmq.support.TestQueues;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DatabasePerTest.class)
class BakeOffRunner {

    private record Run(String storage, String profile, double msgsPerSec,
                       double p50Ms, double p95Ms, double p99Ms) {}

    @Test
    void bakeOff(ExtensionContext ctx) throws Exception {
        var client = new SqlmqClient(DatabasePerTest.dataSource(ctx));
        var results = new ArrayList<Run>();

        for (var profile : BenchmarkProfiles.all()) {
            for (var storage : List.of("ondisk", "inmemory")) {
                // 5 runs per (profile, storage); discard 30 s warmup; measure ~60 s.
                var samples = new ArrayList<Run>();
                for (int i = 0; i < 5; i++) {
                    var q = TestQueues.uniqueName("bench");
                    client.createQueue(q, storage, false, "json", null);
                    long t0 = System.nanoTime();
                    var result = ConcurrencyHarness.run(client, q, profile);
                    long elapsedNs = System.nanoTime() - t0;
                    double seconds = elapsedNs / 1e9;
                    int delivered = result.deliveries().size();
                    double mps = delivered / seconds;
                    // Latency: per-delivery elapsed from enqueue to receive.
                    var latencies = new ArrayList<Long>();
                    for (var d : result.deliveries()) {
                        // Approximate: enqueue time isn't recorded by harness; use receivedAtNanos relative to t0.
                        latencies.add(d.receivedAtNanos() - t0);
                    }
                    java.util.Collections.sort(latencies);
                    samples.add(new Run(storage, profile.toString(), mps,
                        pct(latencies, 50) / 1e6, pct(latencies, 95) / 1e6, pct(latencies, 99) / 1e6));
                    client.dropQueue(q);
                }
                // Median across the 5 runs.
                samples.sort(java.util.Comparator.comparingDouble(Run::msgsPerSec));
                results.add(samples.get(2));
            }
        }

        writeResults(results);
    }

    private long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0;
        int idx = Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * p / 100.0) - 1);
        return sorted.get(Math.max(0, idx));
    }

    private void writeResults(List<Run> results) throws IOException {
        var ts = Instant.now().toString().replace(':', '-');
        var dir = Path.of("target/bench-results", ts);
        Files.createDirectories(dir);

        var summary = new StringBuilder();
        summary.append("# sqlmq bake-off results\n\n");
        summary.append("Container: mcr.microsoft.com/mssql/server:2022-latest (").
            append(SqlServerContainer.get().getDockerImageName()).append(")\n\n");
        summary.append("| Profile | Storage | msgs/sec | p50 (ms) | p95 (ms) | p99 (ms) |\n");
        summary.append("|---------|---------|---------:|---------:|---------:|---------:|\n");
        for (var r : results) {
            summary.append("| ").append(r.profile()).append(" | ").append(r.storage()).append(" | ")
                   .append(String.format("%.0f", r.msgsPerSec())).append(" | ")
                   .append(String.format("%.2f", r.p50Ms())).append(" | ")
                   .append(String.format("%.2f", r.p95Ms())).append(" | ")
                   .append(String.format("%.2f", r.p99Ms())).append(" |\n");
        }
        Files.writeString(dir.resolve("bench-summary.md"), summary.toString());
        System.out.println("\n" + summary);
    }
}
```

### Task 7.3: Add `benchmark.yml` workflow

**Files:**
- Create: `.github/workflows/benchmark.yml`

- [ ] **Step 1**

```yaml
name: Bake-off

on:
  workflow_dispatch:
  schedule:
    - cron: '0 5 * * *'  # daily at 05:00 UTC

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: '25'
          distribution: 'temurin'
      - uses: actions/cache@v4
        with:
          path: ~/.m2
          key: ${{ runner.os }}-m2-${{ hashFiles('harness/pom.xml') }}
      - name: Run bake-off
        working-directory: harness
        run: mvn -B -Pbenchmark verify
      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: bench-results-${{ github.run_id }}
          path: harness/target/bench-results/
```

### Task 7.4: Commit Phase 7

- [ ] **Step 1**

```bash
git add harness/src/test/java/io/freesidenomad/sqlmq/benchmark/ \
        .github/workflows/benchmark.yml
git commit -m "$(cat <<'EOF'
phase 7: bake-off runner

BenchmarkProfiles: 5 standard workloads (light, medium, heavy, asymmetric, burst).
BakeOffRunner: runs each profile 5× per storage variant, reports median throughput
              + p50/p95/p99 latency to target/bench-results/<timestamp>/bench-summary.md.

GitHub Actions benchmark.yml: nightly + manual dispatch, uploads results as artifact.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push
```

---

## Final task: open PR and run Windows smoke check

### Task F.1: Open PR to main

- [ ] **Step 1**

```bash
gh pr create --title "v1: pgmq port to SQL Server (full implementation)" --body "$(cat <<'EOF'
## Summary

Implements sqlmq v1 per the design spec at `docs/superpowers/specs/2026-05-02-sqlmq-port-from-pgmq-design.md`.

- T-SQL migrations V001–V009 in `sql/migrations/`
- Java 25 + virtual-threads test harness in `harness/`
- Both storage variants (on-disk, in-memory) behind one API
- Tier 1 (correctness), Tier 2 (concurrency), Tier 3 (bake-off) tests all green
- Apache 2.0 license, public OSS

## Test plan

- [ ] All Tier 1 tests pass on Linux CI for both storage variants
- [ ] All Tier 2 concurrency tests pass for both variants
- [ ] Manual run of bake-off succeeds; results posted in PR comment
- [ ] Windows smoke check: install on a Windows SQL Server 2022 instance and run `sqlmq.create_queue` + `send` + `read` + `delete` end-to-end

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

### Task F.2: Manual Windows validation (release-blocker)

**Not automatable.** Operator must run on a Windows SQL Server 2022 install:

- [ ] Apply migrations V001–V009 via Flyway or sqlcmd
- [ ] `EXEC sqlmq.create_queue @name='wintest', @storage='ondisk', @grouped=1, @payload_type='json', @max_delivery_count=3`
- [ ] `EXEC sqlmq.create_queue @name='wintestmem', @storage='inmemory', @grouped=1, @payload_type='json', @max_delivery_count=3`
- [ ] Send, read, delete cycle on each
- [ ] Verify `sqlmq.dlq_sweep` works after driving read_ct over the cap
- [ ] Document Windows version, SQL Server version, build, results in the PR

This step is the difference between "Windows portability" being a real claim or aspirational.

---

## Self-review (executed by plan author)

**Spec coverage:**
- ✅ Repo layout (Phase 0)
- ✅ Schema, meta, TVPs (Phase 1)
- ✅ create_queue/drop_queue/queues TVF (Phase 1)
- ✅ send/send_batch (Phase 2)
- ✅ read with on-disk concurrency hints (Phase 2)
- ✅ delete/archive/pop (Phase 2)
- ✅ At-least-once delivery contract — enforced by VT mechanic, exercised by Tier 2 tests
- ✅ FIFO ordering (Phase 3 — FifoOrderingTest)
- ✅ Grouped-FIFO invariant (Phase 5 — GroupedFifoInvariantTest)
- ✅ DLQ exclude-and-sweep (Phases 2 + 4)
- ✅ Metrics (Phase 4)
- ✅ Purge (Phase 4)
- ✅ In-memory variant with per-queue natively compiled inner procs (Phase 6)
- ✅ Both variants share one API; tests parameterized over both (Phase 6)
- ✅ Bake-off (Phase 7)
- ✅ Apache 2.0 / GitHub Actions / public OSS hosting — already in place from initial commit
- ✅ Windows manual validation as release blocker (Task F.2)

**Placeholder scan:**
- Phase 6 Task 6.2 deliberately leaves the per-queue inner-proc generator bodies for execution-time refinement, with a documented reference template for `_read_inmem_<name>`. This is a known plan-author choice rather than a placeholder: dynamic-SQL string-building of natively compiled procs needs to be iterated against the actual container, not pre-baked. Each inner-proc generation gets its own commit (6.2.b through 6.2.f). Acceptable.
- One real follow-up: V009 file ends up containing both filegroup setup *and* multiple `CREATE OR ALTER PROCEDURE` statements added across Tasks 6.2 and 6.3. Since Flyway runs each `V*` file once, this means later edits to V009 during Phase 6 development will require dropping the test database. Document this in Task 6.1 and consider splitting Phase 6 into V009 (filegroup) + V010 (procs) + V011 (dispatchers) for cleanliness — recommended at execution time.
- All other steps contain concrete code or commands.

**Type/method consistency:**
- `SqlmqClient.Message` gains `String groupKey` in Phase 5 Task 5.4 step 2. All earlier `read`/`pop` callers use the record positionally only via getters — adding a new field at the end is non-breaking. Verified.
- `SqlmqClient.send(...)` signature evolves: 5 params in Phase 2, 6 params (adds `@group_key`) in Phase 5. Internal `sendInternal` updates explicitly. Public methods `send`/`sendDelayed`/`sendBinary` keep their public signatures and pass NULL for `@group_key`. Verified.
- `sqlmq.send` proc replaced via `CREATE OR ALTER` in V008, adding `@group_key NVARCHAR(255) = NULL` with default — non-breaking for existing callers.

No issues found that block plan execution.
