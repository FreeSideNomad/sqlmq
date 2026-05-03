package io.freesidenomad.sqlmq.bench;

import io.freesidenomad.sqlmq.bench.ConcurrencyHarness.Profile;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Built-in workload profiles. Mirrors the test-source {@code BenchmarkProfiles}
 * in the {@code benchmark/} package so the standalone CLI can resolve a name
 * like {@code "medium"} to a {@link NamedProfile} without dragging in test code.
 *
 * Profile sizing notes (v1 baseline, May 2026): observed sustained throughput
 * on Testcontainers SQL Server 2022 on an M1 Mac is ~300-500 msgs/sec for both
 * ondisk and inmemory variants. The original {@code Profile.heavy()} (256 producers
 * x 10000 msgs = 2.56M messages) extrapolates to ~95 minutes per run, or ~16 hours
 * for the 5-run-x-2-storage matrix on heavy alone — and triggered an OOM kill of
 * the surefire fork mid-run on a 24GB host. Heavy/asymmetric/burst are scaled down
 * here so the full 5-profile bake-off fits in ~60-90 minutes total.
 */
public final class BenchProfiles {
    private BenchProfiles() {}

    /** Profile + name + how many messages to preload (0 = none, producers do the sending). */
    public record NamedProfile(String name, Profile profile, int preloadCount) {}

    private static final Map<String, NamedProfile> BUILTINS = buildBuiltins();

    private static Map<String, NamedProfile> buildBuiltins() {
        var m = new LinkedHashMap<String, NamedProfile>();
        m.put("light",      new NamedProfile("light",      Profile.light(),                                  0));
        m.put("medium",     new NamedProfile("medium",     Profile.medium(),                                 0));
        m.put("heavy",      new NamedProfile("heavy",      new Profile(64, 64, 1000, 50, 30, 2000),          0));
        m.put("asymmetric", new NamedProfile("asymmetric", new Profile(1, 16, 5000, 100, 30, 2000),          0));
        m.put("burst",      new NamedProfile("burst",      new Profile(0, 128, 0, 50, 30, 2000),         10_000));
        return java.util.Collections.unmodifiableMap(m);
    }

    /** All built-in profiles in display order. */
    public static java.util.Collection<NamedProfile> all() {
        return BUILTINS.values();
    }

    /** Built-in profile names ({@code light}, {@code medium}, ...). */
    public static java.util.Set<String> builtinNames() {
        return BUILTINS.keySet();
    }

    /** Resolve a built-in profile by name. Returns {@code null} if unknown. */
    public static NamedProfile byName(String name) {
        return BUILTINS.get(name);
    }
}
