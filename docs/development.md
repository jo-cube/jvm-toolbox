# Development

## Requirements

- JDK 25, without preview APIs
- The checked-in Gradle wrapper
- `just` for the short command aliases
- Docker Compose only for PostgreSQL integration/performance work

Production code is Java and the published `runtimeClasspath` must remain dependency-free. JUnit, JMH,
HdrHistogram, PostgreSQL JDBC, and related tooling are isolated to test, benchmark, or performance
source sets.

## Repository map

```text
src/main/java/       published batching API and runtime
src/test/java/       behavior and concurrency contract tests
src/jmh/java/        JMH mechanical benchmarks
src/perf/java/       synthetic, profiling, and PostgreSQL tooling
src/perf/postgres/   PostgreSQL schema and seed scripts
docs/                maintained user and contributor documentation
compose.yaml         local PostgreSQL environment
```

`BackendProfiler`, `BatchingProfiler`, reporters, resource probes, synthetic backends, and database
adapters are experimental repository infrastructure. They are not part of the Maven publication.

## Normal workflow

```text
just test                 run unit and concurrency contract tests
just check                compile all source sets, run tests, enforce Javadocs and runtime dependency policy
just docs                 generate warning-free API Javadocs
just publication-check    build binary/source/Javadoc artifacts, Maven POM, and Gradle metadata
```

`just check` is the expected pre-commit command. It compiles the JMH and performance source sets but
does not execute expensive benchmarks or require Docker.

Performance commands are listed in [performance.md](performance.md). PostgreSQL unit-independent
workflows use `postgres-up`, `postgres-seed`, the relevant `perf-postgres*` command, and
`postgres-down`.

## Contract and documentation ownership

The published Java types and their Javadocs define the public surface. Behavior and concurrency tests
freeze observable contracts. Maintained Markdown documentation should explain how to apply those
contracts without introducing different semantics.

Each maintained document has one primary job:

| Document | Canonical subject |
| --- | --- |
| `README.md` | Project discovery, installation, abstraction choice, and the application lifecycle at a glance. |
| `docs/batching.md` | Shared batching contracts, non-keyed processing, API-server integration, overload, and lifecycle. |
| `docs/key-batch-loader.md` | Keyed backend integration, coalescing, missing values, fan-out, and cache scope. |
| `docs/performance.md` | Measurement methodology, evidence, configuration workflow, and performance commands. |
| `docs/development.md` | Repository workflow, contribution rules, documentation maintenance, and publication. |
| `AGENTS.md` | Short routing and invariants for coding agents; it points to the maintained guides for details. |

When behavior changes, update the source Javadocs, focused behavior tests, and the one canonical guide
in the same change. Link to shared explanations instead of copying them between guides. Keep examples
small enough to audit, explicitly label framework placeholders, and show long-lived batching instances
rather than creating one per request.

Generated Javadocs under `build/docs/javadoc` and performance reports under `build/reports` are build
artifacts, not maintained documentation.

## Engineering expectations

- Keep public APIs smaller than their implementations and independent of queues, threads, schedulers,
  JDBC, and monitoring frameworks.
- Treat admission, timing, result correlation, failure, cancellation, concurrency bounds, and shutdown
  as contracts.
- Add behavior tests for observable guarantees and concurrency invariants. Do not chase coverage
  percentages or test private structure.
- Prefer deterministic clocks, latches, barriers, and semaphores over sleep-heavy concurrency tests.
- Public Javadocs describe nullability, ownership, blocking, exceptions, lifecycle, and concurrency.
  Comments explain only non-obvious invariants or memory/concurrency reasoning.
- Measure performance changes with JMH or the appropriate system harness before changing production
  internals. Do not add throughput thresholds to ordinary CI.
- New toolbox features must be coherent reusable abstractions with standalone value, not miscellaneous
  shared code or catch-all utility packages.

## Publication

The `mavenJava` publication produces `io.github.jo-cube:jvm-toolbox` with binary, source, Javadoc,
Gradle module, license, project, developer, and SCM metadata. Local builds use
`0.0.0-SNAPSHOT`; pass `-PreleaseVersion=1.2.3` when a specific version is needed. The release
workflow supplies this property from the published GitHub Release's `v1.2.3` tag, so release versions
are not committed to the build file.

CI and release automation are deliberately separate:

| Workflow | Trigger | Responsibility |
| --- | --- | --- |
| `CI` | Pull requests, pushes to `main`, manual dispatch | Run `check` and build publication metadata without release credentials. |
| `Release` | Published GitHub Releases | Validate the release tag on `main`, repeat verification, then sign and publish through the Maven Central Portal. |

The release workflow accepts tags such as `v1.2.3` and `v1.2.3-alpha.1`; Maven receives the version
without the leading `v`. A Central release is immutable, so never move or reuse a published version
tag.

### One-time GitHub and Maven Central setup

1. Generate a user token in the [Maven Central Portal](https://central.sonatype.com/usertoken).
2. Create a passphrase-protected PGP key, publish its public key to a Central-supported key server,
   and export the armored private key:

   ```text
   gpg --gen-key
   gpg --keyserver keyserver.ubuntu.com --send-keys KEY_ID
   gpg --export-secret-keys --armor KEY_ID
   ```

3. Create a GitHub environment named `maven-central` and add these environment secrets:

   | Secret | Value |
   | --- | --- |
   | `MAVEN_CENTRAL_USERNAME` | Username from the generated Central Portal user token. |
   | `MAVEN_CENTRAL_PASSWORD` | Password from the generated Central Portal user token. |
   | `SIGNING_KEY` | Complete armored private key, including the begin/end lines. |
   | `SIGNING_PASSWORD` | Private-key passphrase. |

4. Restrict the `maven-central` environment to tags matching `v*`. Requiring approval is useful for
   early releases because the workflow publishes automatically after Central validates the bundle.
5. Protect `main`: require pull requests and the `CI / verify` check, and restrict creation or update
   of `v*` tags to maintainers.

### Releasing

Merge the feature branch through a pull request, update local `main`, then tag the exact commit:

```text
git switch main
git pull --ff-only
git tag -a v0.1.0-alpha.1 -m "v0.1.0-alpha.1"
git push origin v0.1.0-alpha.1
```

Create a GitHub Release for that tag and publish it; pushing the tag alone does not publish to Maven
Central. The release workflow rejects malformed tags and commits that are not contained in
`origin/main`. After the protected environment is approved, it signs the artifacts, uploads a
deployment, waits for Central validation, and releases it automatically. Maven Central synchronization
can take additional time after the workflow succeeds.

`./gradlew publishToMavenLocal` remains available for local consumer testing. For a release-shaped
local artifact, use `./gradlew publishToMavenLocal -PreleaseVersion=1.2.3` and configure local signing
credentials as described by Gradle's signing plugin.

Before preparing a prerelease:

1. Confirm the intended version and public API documentation.
2. Run `just check` and the smallest relevant performance or integration smoke workflow.
3. Run `just publication-check`.
4. Run `./gradlew dependencies --configuration runtimeClasspath` and confirm that production reports
   no dependencies.
5. Inspect the generated POM and binary, source, and Javadoc JARs under `build/`.
6. Use `publishToMavenLocal` when a separate consumer project should verify coordinates and Java 25
   metadata.

Normal CI repeats correctness, warning-free Javadocs, source-set compilation, runtime dependency
isolation, and publication artifact generation. It does not run Docker or assert performance
thresholds.
