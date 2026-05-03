package io.freesidenomad.sqlmq.support;

import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

/**
 * Provides storage variants as test arguments so that parameterized tests can
 * run their body against each.
 *
 * <p>As of V014 the in-memory variant has been retired (see V014 header for
 * rationale: in-memory never beat on-disk in the bake-off). {@link #all()} now
 * yields only {@code "ondisk"}. The parameterization machinery stays in place
 * so adding a future storage variant remains a one-line change here.</p>
 *
 * Usage:
 * <pre>
 * &#64;ParameterizedTest
 * &#64;MethodSource("io.freesidenomad.sqlmq.support.StorageVariants#all")
 * void someTest(String storage, ExtensionContext ctx) throws Exception { ... }
 * </pre>
 */
public final class StorageVariants {
    private StorageVariants() {}

    public static Stream<Arguments> all() {
        return Stream.of(Arguments.of("ondisk"));
    }
}
