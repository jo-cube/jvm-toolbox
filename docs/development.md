# Development

## Requirements

- JDK 25, without preview APIs
- The checked-in Gradle wrapper
- `just` for short command aliases
- Docker Compose for consumer Kafka integration and batching PostgreSQL performance work

The root is an unpublished Gradle aggregator. `batching` and `bulkhead` are dependency-free
artifacts; `consumers` intentionally depends on and exposes the Apache Kafka client API. JUnit, JMH,
HdrHistogram, PostgreSQL JDBC, and profiling dependencies remain outside production source sets.

## Repository map

```text
batching/src/main/java/       batching published API and runtime
batching/src/test/java/       batching behavior and concurrency tests
batching/src/jmh/java/        JMH mechanical benchmarks
batching/src/perf/            synthetic, profiling, and PostgreSQL tooling
batching/docs/                batching and performance guides
bulkhead/src/main/java/       bulkhead published API and runtime
bulkhead/src/test/java/       bulkhead behavior and concurrency tests
bulkhead/src/jmh/java/        bulkhead mechanical benchmarks
bulkhead/docs/                bulkhead guide
consumers/src/main/java/      consumers published API and runtime
consumers/src/test/java/      consumer behavior and concurrency tests
consumers/src/integrationTest/ broker-backed consumer integration tests
consumers/docs/               ordered-consumer guide
docs/development.md           repository and publication workflow
compose.yaml                  local Kafka and PostgreSQL environments
```

Repository-only profilers, reporters, probes, synthetic backends, and database adapters are not part
of any Maven publication.

## Normal workflow

```text
just test                 run all unit and concurrency contract tests
just check                compile without warnings, run tests, and enforce warning-free Javadocs
just docs                 generate all artifacts' API Javadocs
just publication-check    build artifacts, POMs, and Gradle metadata
```

`just check` is the expected pre-commit command. It compiles the batching JMH and performance source
sets and the consumer integration-test source set, but does not execute external-system workflows or
require Docker. Performance commands are documented in
[performance.md](../batching/docs/performance.md).

## Kafka integration

Kafka provisioning and test execution are independent:

```text
just kafka-up
just integration-test
just kafka-down
```

The Gradle `integrationTest` suite expects Kafka at `localhost:59092`; it does not manage Docker.
Set `JVM_TOOLBOX_KAFKA_PORT` to change the Compose host port, or
`JVM_TOOLBOX_KAFKA_BOOTSTRAP_SERVERS` to run the suite against another broker. CI provisions the
pinned Kafka service separately and runs the suite as its own job.

## Contract and documentation ownership

Published Java types and Javadocs define the public surface. Behavior and concurrency tests freeze
observable contracts. Maintained Markdown explains how to apply those contracts.

| Document | Canonical subject |
| --- | --- |
| `README.md` | Project discovery, installation, and artifact choice. |
| `batching/docs/batching.md` | Shared batching contracts, non-keyed processing, admission, and lifecycle. |
| `batching/docs/windowed-accumulator.md` | Incremental accumulation, windows, ownership, backpressure, and lifecycle. |
| `batching/docs/key-batch-loader.md` | Keyed processing, coalescing, missing values, fan-out, and cache scope. |
| `batching/docs/single-flight.md` | Full-flight request coalescing, cancellation, failure, and cache scope. |
| `batching/docs/performance.md` | Measurement methodology, evidence, and performance commands. |
| `bulkhead/docs/bulkhead.md` | Capacity, admission, execution, cancellation, and scope. |
| `consumers/docs/consumers.md` | Routing, ordering, batching, offsets, backpressure, rebalance, and shutdown. |
| `docs/development.md` | Repository, CI, documentation, and publication workflow. |
| `AGENTS.md` | Short routing and invariants for coding agents. |

When behavior changes, update Javadocs, focused behavior tests, and the canonical guide together. Link
to shared explanations rather than copying them. Generated Javadocs and reports under each module's
`build/` directory are build artifacts, not maintained documentation.

## Engineering expectations

- Keep public APIs smaller than their implementations and independent of internal queues, threads,
  schedulers, JDBC adapters, and monitoring frameworks.
- Treat admission, timing, correlation, failure, cancellation, concurrency bounds, offsets,
  rebalance fencing, commits, and shutdown as observable contracts where applicable.
- Prefer deterministic clocks, latches, barriers, and semaphores over sleep-heavy concurrency tests.
- Public Javadocs describe nullability, ownership, blocking, exceptions, lifecycle, and concurrency.
- Measure performance changes with the appropriate module's JMH or system harness before changing
  performance-sensitive internals. Do not add machine-specific thresholds to ordinary CI.
- Add only coherent reusable abstractions with standalone value; do not add catch-all shared modules.

## Publication

The `mavenJava` publications produce:

- `io.github.jo-cube:jvm-toolbox-batching` from `batching`.
- `io.github.jo-cube:jvm-toolbox-bulkhead` from `bulkhead`.
- `io.github.jo-cube:jvm-toolbox-consumers` from `consumers`.

All include binary, source, Javadoc, Gradle module, license, project, developer, and SCM metadata.
Local builds use `0.0.0-SNAPSHOT`; pass `-PreleaseVersion=1.2.3` for a specific version. The release
workflow derives that value from the published GitHub Release's `v1.2.3` tag.

CI and release automation are separate:

| Workflow | Trigger | Responsibility |
| --- | --- | --- |
| `CI` | Pull requests, pushes to `main`, manual dispatch | Run `check`, Kafka integration tests, and build all publications without release credentials. |
| `Release` | Published GitHub Releases | Validate the release tag on `main`, verify, sign, and publish all artifacts. |

Release tags may look like `v1.2.3` or `v1.2.3-alpha.1`; Maven receives the value without the leading
`v`. Central releases are immutable, so never move or reuse a published version tag.

### One-time GitHub and Maven Central setup

1. Generate a user token in the [Maven Central Portal](https://central.sonatype.com/usertoken).
2. Create a passphrase-protected PGP key, publish its public key, and export the armored private key.
3. Create a GitHub environment named `maven-central` with `MAVEN_CENTRAL_USERNAME`,
   `MAVEN_CENTRAL_PASSWORD`, `SIGNING_KEY`, and `SIGNING_PASSWORD` secrets.
4. Restrict that environment to `v*` tags; approval is useful for early releases.
5. Protect `main` and require the `CI / verify` and `CI / kafka-integration` checks.

### Releasing

Tag the exact commit on `main`, push the tag, then create and publish its GitHub Release. Pushing the
tag alone does not publish. The workflow validates that the tagged commit belongs to `main`, signs all
three publications, waits for Central validation, and releases the deployment.

`./gradlew publishToMavenLocal` publishes all three modules for local consumer testing. A release-shaped
local build can add `-PreleaseVersion=1.2.3` and Gradle signing credentials.

Before preparing a release:

1. Confirm the version and public API documentation.
2. Run `just check` and the smallest relevant performance or integration smoke workflow.
3. Run `just publication-check`.
4. Run `./gradlew :batching:dependencies :bulkhead:dependencies --configuration runtimeClasspath`
   and confirm both have no dependencies.
5. Inspect the generated POMs and JAR sets under each module's `build/` directory; the consumers POM
   must declare `kafka-clients`.
6. Use `publishToMavenLocal` when a separate consumer project should verify coordinates and Java 25
   metadata.
