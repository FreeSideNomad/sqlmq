package io.freesidenomad.sqlmq.client;

import java.time.Instant;
import java.util.Map;

/**
 * A single queued message.
 *
 * <p>Mirrors the Python client's {@code sqlmq.Message} (which mirrors
 * {@code tembo_pgmq_python.Message}). The {@link #message} field is the
 * decoded JSON payload as a {@code Map<String, Object>}, not the raw stored
 * NVARCHAR.</p>
 *
 * <p>{@link #headers} holds the optional header map (also stored as JSON
 * NVARCHAR). May be {@code null} if the row has no headers.</p>
 */
public record Message(
    long msgId,
    int readCt,
    Instant enqueuedAt,
    Instant vt,
    Map<String, Object> message,
    Map<String, Object> headers
) {}
