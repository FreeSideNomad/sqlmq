package io.freesidenomad.sqlmq.benchmark;

import io.freesidenomad.sqlmq.support.ConcurrencyHarness.Profile;

import java.util.List;

public final class BenchmarkProfiles {
    private BenchmarkProfiles() {}

    public static List<NamedProfile> all() {
        return List.of(
            new NamedProfile("light",      Profile.light(),       0),
            new NamedProfile("medium",     Profile.medium(),      0),
            new NamedProfile("heavy",      Profile.heavy(),       0),
            new NamedProfile("asymmetric", Profile.asymmetric(),  0),
            // Burst: preload 100k messages, then 1024 consumers race to drain.
            new NamedProfile("burst",      new Profile(0, 1024, 0, 50, 30, 2000), 100_000)
        );
    }

    /** Profile + name + how many messages to preload (0 = none, producers do the sending). */
    public record NamedProfile(String name, Profile profile, int preloadCount) {}
}
