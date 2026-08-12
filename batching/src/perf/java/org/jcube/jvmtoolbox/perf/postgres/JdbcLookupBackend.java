package org.jcube.jvmtoolbox.perf.postgres;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.LongSupplier;
import org.jcube.jvmtoolbox.batching.BatchOutcome;

final class JdbcLookupBackend implements AutoCloseable {
    static final String LOOKUP_SQL = """
            SELECT stored.key_text, stored.key_number, stored.value
            FROM unnest(?::text[], ?::integer[]) AS requested(key_text, key_number)
            JOIN lookup_value AS stored USING (key_text, key_number)
            """;

    private final ArrayBlockingQueue<Session> sessions;
    private final PostgresMetrics metrics;
    private final Slowdown slowdown;
    private final LongSupplier elapsedNanos;

    JdbcLookupBackend(PostgresSettings settings, int concurrency) throws SQLException {
        this(settings, concurrency, null, Slowdown.NONE, () -> 0);
    }

    JdbcLookupBackend(
            PostgresSettings settings,
            int concurrency,
            PostgresMetrics metrics,
            Slowdown slowdown,
            LongSupplier elapsedNanos)
            throws SQLException {
        this.metrics = metrics;
        this.slowdown = slowdown;
        this.elapsedNanos = elapsedNanos;
        sessions = new ArrayBlockingQueue<>(concurrency);
        try {
            for (int index = 0; index < concurrency; index++) {
                sessions.add(openSession(settings));
            }
        } catch (SQLException failure) {
            close();
            throw failure;
        }
    }

    Map<LookupKey, BatchOutcome<BigDecimal>> load(Collection<LookupKey> keys) throws Exception {
        Session session = sessions.take();
        long started = System.nanoTime();
        boolean failed = true;
        if (metrics != null) {
            metrics.databaseStarted();
        }
        try {
            applySlowdown(session);
            Map<LookupKey, BatchOutcome<BigDecimal>> results = query(session.connection(), session.lookup(), keys);
            failed = false;
            return results;
        } finally {
            if (metrics != null) {
                metrics.databaseFinished(keys.size(), started, failed);
            }
            sessions.add(session);
        }
    }

    @Override
    public void close() {
        Session session;
        while ((session = sessions.poll()) != null) {
            session.close();
        }
    }

    private void applySlowdown(Session session) throws SQLException {
        if (!slowdown.active(elapsedNanos.getAsLong())) {
            return;
        }
        session.delay().setDouble(1, slowdown.delay().toNanos() / 1_000_000_000d);
        session.delay().execute();
    }

    private static Session openSession(PostgresSettings settings) throws SQLException {
        Connection connection = settings.open();
        try {
            return new Session(
                    connection,
                    connection.prepareStatement(LOOKUP_SQL),
                    connection.prepareStatement("SELECT pg_sleep(?)"));
        } catch (SQLException failure) {
            connection.close();
            throw failure;
        }
    }

    static DatasetInfo datasetInfo(PostgresSettings settings) throws SQLException {
        try (Connection connection = settings.open();
                var statement = connection.createStatement();
                ResultSet result = statement.executeQuery("""
                        SELECT count(*), pg_total_relation_size('lookup_value')
                        FROM lookup_value
                        """)) {
            result.next();
            return new DatasetInfo(result.getLong(1), result.getLong(2));
        }
    }

    static List<String> explain(PostgresSettings settings, Set<LookupKey> keys) throws SQLException {
        try (Connection connection = settings.open();
                PreparedStatement statement = connection.prepareStatement(
                        "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT) " + LOOKUP_SQL);
                BoundArrays ignored = bind(connection, statement, keys);
                ResultSet result = statement.executeQuery()) {
                var lines = new ArrayList<String>();
                while (result.next()) {
                    lines.add(result.getString(1));
                }
                return List.copyOf(lines);
        }
    }

    private static Map<LookupKey, BatchOutcome<BigDecimal>> query(
            Connection connection, PreparedStatement statement, Collection<LookupKey> keys) throws SQLException {
        try (BoundArrays ignored = bind(connection, statement, keys);
                ResultSet result = statement.executeQuery()) {
            var values = new HashMap<LookupKey, BatchOutcome<BigDecimal>>(keys.size());
            while (result.next()) {
                values.put(
                        new LookupKey(result.getString(1), result.getInt(2)),
                        BatchOutcome.success(result.getBigDecimal(3)));
            }
            return values;
        }
    }

    private static BoundArrays bind(
            Connection connection, PreparedStatement statement, Collection<LookupKey> keys)
            throws SQLException {
        String[] textKeys = new String[keys.size()];
        Integer[] numberKeys = new Integer[keys.size()];
        int index = 0;
        for (LookupKey key : keys) {
            textKeys[index] = key.text();
            numberKeys[index] = key.number();
            index++;
        }
        Array texts = connection.createArrayOf("text", textKeys);
        Array numbers = connection.createArrayOf("integer", numberKeys);
        statement.setArray(1, texts);
        statement.setArray(2, numbers);
        return new BoundArrays(texts, numbers);
    }

    private record Session(Connection connection, PreparedStatement lookup, PreparedStatement delay) {
        void close() {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }

    private record BoundArrays(Array texts, Array numbers) implements AutoCloseable {
        @Override
        public void close() throws SQLException {
            texts.free();
            numbers.free();
        }
    }
}

record LookupKey(String text, int number) {}

record DatasetInfo(long rows, long relationBytes) {}

record Slowdown(Duration start, Duration duration, Duration delay) {
    static final Slowdown NONE = new Slowdown(Duration.ZERO, Duration.ZERO, Duration.ZERO);

    boolean active(long elapsedNanos) {
        return !duration.isZero()
                && elapsedNanos >= start.toNanos()
                && elapsedNanos < start.plus(duration).toNanos();
    }
}

record PostgresSettings(String url, String user, String password) {
    static PostgresSettings environment() {
        return new PostgresSettings(
                System.getenv().getOrDefault(
                        "JVM_TOOLBOX_POSTGRES_URL",
                        "jdbc:postgresql://localhost:55432/jvm_toolbox"),
                System.getenv().getOrDefault("JVM_TOOLBOX_POSTGRES_USER", "jvm_toolbox"),
                System.getenv().getOrDefault("JVM_TOOLBOX_POSTGRES_PASSWORD", "jvm_toolbox"));
    }

    Connection open() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
