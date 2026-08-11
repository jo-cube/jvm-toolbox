test:
    ./gradlew test

check:
    ./gradlew check

docs:
    ./gradlew javadoc

publication-check:
    ./gradlew assemble :batching:generatePomFileForMavenJavaPublication :batching:generateMetadataFileForMavenJavaPublication :consumers:generatePomFileForMavenJavaPublication :consumers:generateMetadataFileForMavenJavaPublication

bench-quick:
    ./gradlew :batching:jmh -PbenchmarkProfile=quick

bench:
    ./gradlew :batching:jmh -PbenchmarkProfile=full

bench-allocation:
    ./gradlew :batching:jmh -PbenchmarkProfile=allocation

bench-jfr:
    ./gradlew :batching:jmh -PbenchmarkProfile=jfr

perf-quick:
    ./gradlew :batching:perf -PperfArgs=quick

perf:
    ./gradlew :batching:perf -PperfArgs=full

perf-custom *args:
    ./gradlew :batching:perf -PperfArgs='custom {{args}}'

perf-jfr:
    ./gradlew :batching:perfJfr

perf-backend-profiler:
    ./gradlew :batching:perfBackendProfiler

perf-batching-profiler:
    ./gradlew :batching:perfBatchingProfiler

postgres-up:
    docker compose up -d --wait

postgres-seed rows="100000":
    docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -v row_count={{rows}} -U jvm_toolbox -d jvm_toolbox -f /perf/seed.sql

perf-postgres:
    ./gradlew :batching:perfPostgres -PpostgresProfile=quick

perf-postgres-full:
    ./gradlew :batching:perfPostgres -PpostgresProfile=full

perf-postgres-jfr:
    ./gradlew :batching:perfPostgresJfr

perf-postgres-attribution:
    ./gradlew :batching:perfPostgresAttribution

perf-postgres-attribution-jfr:
    ./gradlew :batching:perfPostgresAttributionJfr

perf-postgres-batching-profiler:
    ./gradlew :batching:perfPostgresBatchingProfiler

postgres-plan:
    ./gradlew :batching:perfPostgres -PpostgresProfile=plan

postgres-down:
    docker compose down
