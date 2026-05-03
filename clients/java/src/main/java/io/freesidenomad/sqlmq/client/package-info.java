/**
 * Strict pgmq-API-compatible Java client for sqlmq on SQL Server.
 *
 * <p>Public surface mirrors the sqlmq-python client (which mirrors
 * tembo-pgmq-python 0.10.0) so that Python and Java consumers can use
 * the same call sites and method semantics.</p>
 *
 * <p>Methods that have no sqlmq equivalent
 * ({@code unlogged=true}, {@code createPartitionedQueue},
 * {@code setVt}, {@code detachArchive}) throw
 * {@link java.lang.UnsupportedOperationException} with the same wording
 * as the Python client's {@code NotImplementedError}.</p>
 *
 * <h2>Sync vs async</h2>
 *
 * <p>{@link io.freesidenomad.sqlmq.client.PgmqClient} is the synchronous client,
 * built on {@code mssql-jdbc}. {@link io.freesidenomad.sqlmq.client.AsyncPgmqClient}
 * wraps the sync client in {@link java.util.concurrent.CompletableFuture#supplyAsync}
 * dispatched to an {@link java.util.concurrent.Executor}. This is thread-pool
 * dispatch, NOT true non-blocking I/O. R2DBC-mssql is the alternative; rejected
 * for v0.1 to keep the dep tree small and to match the Python lib's stance.</p>
 */
package io.freesidenomad.sqlmq.client;
