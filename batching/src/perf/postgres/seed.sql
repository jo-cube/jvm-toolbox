\if :{?row_count}
\else
\set row_count 100000
\endif

TRUNCATE lookup_value;

INSERT INTO lookup_value (key_text, key_number, value)
SELECT
    'tenant-' || (ordinal / 1000),
    (ordinal % 1000)::integer,
    (ordinal::numeric / 100)::numeric(20, 2)
FROM generate_series(0, :row_count - 1) AS ordinal;

ANALYZE lookup_value;

SELECT count(*) AS seeded_rows, pg_size_pretty(pg_total_relation_size('lookup_value')) AS relation_size
FROM lookup_value;
