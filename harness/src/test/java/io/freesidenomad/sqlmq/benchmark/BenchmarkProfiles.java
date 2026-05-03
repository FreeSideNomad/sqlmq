package io.freesidenomad.sqlmq.benchmark;

import io.freesidenomad.sqlmq.bench.ConcurrencyHarness.Profile;

import java.util.List;

public final class BenchmarkProfiles {
    private BenchmarkProfiles() {}

    public static List<NamedProfile> all() {
        // Profile sizing notes: observed sustained throughput on Testcontainers SQL Server
        // 2022 on an M1 Mac is ~300-500 msgs/sec on the on-disk variant. The original
        // Profile.heavy() (256 producers x 10000 msgs = 2.56M messages) extrapolates to
        // ~95 minutes per run, or ~16 hours for the 5-run matrix on heavy alone — and
        // triggered an OOM kill of the surefire fork mid-run on a 24GB host.
        // Heavy/asymmetric/burst are scaled down here so the full 5-profile bake-off
        // fits in ~30-45 minutes total against on-disk only.
        return List.of(
            new NamedProfile("light",      Profile.light(),                                  0),
            new NamedProfile("medium",     Profile.medium(),                                 0),
            // heavy: 64 producers x 1000 msgs (was 256 x 10000) = 64k msgs
            new NamedProfile("heavy",      new Profile(64, 64, 1000, 50, 30, 2000),          0),
            // asymmetric: 1 producer x 5000 msgs, 16 consumers (was 1 x 100000, 64)
            new NamedProfile("asymmetric", new Profile(1, 16, 5000, 100, 30, 2000),          0),
            // burst: preload 10k messages, 128 consumers race to drain (was 100k, 1024)
            new NamedProfile("burst",      new Profile(0, 128, 0, 50, 30, 2000),         10_000)
        );
    }

    /** Profile + name + how many messages to preload (0 = none, producers do the sending). */
    public record NamedProfile(String name, Profile profile, int preloadCount) {}
}
