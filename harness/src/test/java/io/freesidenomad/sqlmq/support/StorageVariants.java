package io.freesidenomad.sqlmq.support;

import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

/**
 * Provides {@code "ondisk"} and {@code "inmemory"} as test arguments so that
 * the same test body can be run against both storage variants.
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
        return Stream.of(Arguments.of("ondisk"), Arguments.of("inmemory"));
    }
}
