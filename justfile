test:
    ./gradlew test

check:
    ./gradlew check

docs:
    ./gradlew javadoc

publication-check:
    ./gradlew assemble generatePomFileForMavenJavaPublication

bench-quick:
    ./gradlew jmh -PbenchmarkProfile=quick

bench:
    ./gradlew jmh -PbenchmarkProfile=full

bench-allocation:
    ./gradlew jmh -PbenchmarkProfile=allocation

bench-jfr:
    ./gradlew jmh -PbenchmarkProfile=jfr

perf-quick:
    ./gradlew perf -PperfArgs=quick

perf:
    ./gradlew perf -PperfArgs=full

perf-custom *args:
    ./gradlew perf -PperfArgs='custom {{args}}'

perf-jfr:
    ./gradlew perfJfr

perf-backend-profiler:
    ./gradlew perfBackendProfiler

perf-batching-profiler:
    ./gradlew perfBatchingProfiler

postgres-up:
    docker compose up -d --wait

postgres-seed rows="100000":
    docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -v row_count={{rows}} -U jvm_toolbox -d jvm_toolbox -f /perf/seed.sql

perf-postgres:
    ./gradlew perfPostgres -PpostgresProfile=quick

perf-postgres-full:
    ./gradlew perfPostgres -PpostgresProfile=full

perf-postgres-jfr:
    ./gradlew perfPostgresJfr

perf-postgres-attribution:
    ./gradlew perfPostgresAttribution

perf-postgres-attribution-jfr:
    ./gradlew perfPostgresAttributionJfr

perf-postgres-batching-profiler:
    ./gradlew perfPostgresBatchingProfiler

postgres-plan:
    ./gradlew perfPostgres -PpostgresProfile=plan

postgres-down:
    docker compose down
