package io.freesidenomad.sqlmq.client.internal;

import java.util.regex.Pattern;

/**
 * Client-side queue name validation, mirroring sqlmq.create_queue.
 *
 * <p>Names must start with a letter or underscore, contain only
 * {@code [A-Za-z0-9_]}, and be at most 60 characters (to leave room for
 * derived constraint and index names within SYSNAME's 128-char limit).</p>
 *
 * <p>Failing fast client-side avoids a server round trip and surfaces a
 * clear error type (an {@link IllegalArgumentException}) rather than a
 * driver-level {@code SQLException} that callers would have to unpack.</p>
 */
public final class QueueNameValidator {

    private static final Pattern VALID = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]{0,59}$");

    private QueueNameValidator() {}

    /**
     * Validate a queue name.
     *
     * @throws IllegalArgumentException if the name is null, empty, or fails
     *         the regex.
     */
    public static void validate(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("queue name must be non-empty");
        }
        if (!VALID.matcher(name).matches()) {
            throw new IllegalArgumentException(
                "invalid queue name '" + name + "': must match "
                    + "^[A-Za-z_][A-Za-z0-9_]{0,59}$ "
                    + "(start with letter/underscore, then letters/digits/underscores, max 60 chars)"
            );
        }
    }
}
