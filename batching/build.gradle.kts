plugins {
    `java-library`
    id("com.vanniktech.maven.publish.base")
    id("me.champeau.jmh")
}

description = "Bounded micro-batching and keyed batch loading"

base {
    archivesName = "jvm-toolbox-batching"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:6.1.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val perfSourceSet = sourceSets.create("perf") {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

dependencies {
    add(perfSourceSet.implementationConfigurationName, "org.hdrhistogram:HdrHistogram:2.2.2")
    add(perfSourceSet.implementationConfigurationName, "org.postgresql:postgresql:42.7.13")
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 25
}

tasks.withType<Jar>().configureEach {
    from(rootProject.file("LICENSE")) {
        into("META-INF")
    }
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addBooleanOption("Werror", true)
}

tasks.test {
    useJUnitPlatform()
}

val verifyRuntimeClasspath = tasks.register("verifyRuntimeClasspath") {
    group = "verification"
    description = "Verifies that the published runtime has no dependencies"
    doLast {
        val runtimeFiles = configurations.runtimeClasspath.get().files
        check(runtimeFiles.isEmpty()) {
            "production runtimeClasspath must remain dependency-free: ${runtimeFiles.joinToString()}"
        }
    }
}

tasks.named("check") {
    dependsOn("javadoc", perfSourceSet.classesTaskName, "jmhClasses", verifyRuntimeClasspath)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "jvm-toolbox-batching"

            pom {
                name = "jvm-toolbox-batching"
                description = project.description
                url = "https://github.com/jo-cube/jvm-toolbox"
                inceptionYear = "2026"

                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/license/mit"
                        distribution = "repo"
                    }
                }
                developers {
                    developer {
                        id = "jo-cube"
                        name = "Joshwin John Jose"
                    }
                }
                scm {
                    connection = "scm:git:https://github.com/jo-cube/jvm-toolbox.git"
                    developerConnection = "scm:git:ssh://git@github.com/jo-cube/jvm-toolbox.git"
                    url = "https://github.com/jo-cube/jvm-toolbox"
                }
            }
        }
    }
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()
}

tasks.register<JavaExec>("perf") {
    group = "verification"
    description = "Runs the synthetic batching system workload"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.BatchingLoadHarness"
    args(providers.gradleProperty("perfArgs").orElse("quick").get().split(' '))
}

tasks.register<JavaExec>("perfBackendProfiler") {
    group = "verification"
    description = "Runs the reusable backend profiler against a synthetic backend"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.backend.BackendProfilerHarness"
}

tasks.register<JavaExec>("perfBatchingProfiler") {
    group = "verification"
    description = "Runs the reusable batching profiler against synthetic backends"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.BatchingProfilerHarness"
}

tasks.register<JavaExec>("perfJfr") {
    group = "verification"
    description = "Profiles one representative synthetic batching workload with JFR"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.BatchingLoadHarness"
    args("profile")
    doFirst {
        project.file("build/reports/perf").mkdirs()
    }
    jvmArgs(
        "-XX:StartFlightRecording=filename=build/reports/perf/profile.jfr,settings=profile,dumponexit=true")
}

tasks.register<JavaExec>("perfPostgres") {
    group = "verification"
    description = "Runs the PostgreSQL bulk lookup validation"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.postgres.PostgresLoadHarness"
    args(providers.gradleProperty("postgresProfile").orElse("quick").get())
}

tasks.register<JavaExec>("perfPostgresJfr") {
    group = "verification"
    description = "Profiles one representative PostgreSQL keyed workload with JFR"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.postgres.PostgresLoadHarness"
    args("profile")
    doFirst {
        project.file("build/reports/perf").mkdirs()
    }
    jvmArgs(
        "-XX:StartFlightRecording=filename=build/reports/perf/postgres-profile.jfr,settings=profile,dumponexit=true")
}

tasks.register<JavaExec>("perfPostgresAttribution") {
    group = "verification"
    description = "Runs the PostgreSQL direct/batching attribution study"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.postgres.PostgresLoadHarness"
    args("attribution")
}

tasks.register<JavaExec>("perfPostgresAttributionJfr") {
    group = "verification"
    description = "Profiles the PostgreSQL direct/batching attribution study with JFR"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.postgres.PostgresLoadHarness"
    args("attribution-profile")
    doFirst {
        project.file("build/reports/perf").mkdirs()
    }
    jvmArgs(
        "-XX:StartFlightRecording=filename=build/reports/perf/postgres-attribution.jfr,settings=profile,dumponexit=true")
}

tasks.register<JavaExec>("perfPostgresBatchingProfiler") {
    group = "verification"
    description = "Runs targeted PostgreSQL experiments through the batching profiler"
    classpath = perfSourceSet.runtimeClasspath
    mainClass = "org.jcube.jvmtoolbox.perf.postgres.PostgresBatchingProfilerHarness"
}

val benchmarkProfile = providers.gradleProperty("benchmarkProfile").orElse("full").get()

jmh {
    jmhVersion = "1.37"
    failOnError = true
    resultFormat = "JSON"
    resultsFile = project.file("build/reports/jmh/$benchmarkProfile-results.json")
    humanOutputFile = project.file("build/reports/jmh/$benchmarkProfile-human.txt")

    when (benchmarkProfile) {
        "quick" -> {
            warmupIterations = 1
            warmup = "250ms"
            iterations = 2
            timeOnIteration = "250ms"
            fork = 1
            benchmarkMode = listOf("avgt")
            timeUnit = "us"
        }
        "allocation" -> {
            warmupIterations = 2
            warmup = "500ms"
            iterations = 3
            timeOnIteration = "500ms"
            fork = 1
            benchmarkMode = listOf("avgt")
            timeUnit = "us"
            profilers = listOf("gc")
        }
        "jfr" -> {
            warmupIterations = 2
            warmup = "1s"
            iterations = 3
            timeOnIteration = "1s"
            fork = 1
            benchmarkMode = listOf("avgt")
            timeUnit = "us"
            includes = listOf(".*MicroBatcherBenchmark.batch")
            benchmarkParameters.put(
                "batchSize", objects.listProperty(String::class.java).value(listOf("128")))
            benchmarkParameters.put(
                "statisticsEnabled", objects.listProperty(String::class.java).value(listOf("false")))
            profilers = listOf("jfr:dir=build/reports/jmh/jfr;configName=profile")
        }
        "full" -> {
            warmupIterations = 3
            warmup = "1s"
            iterations = 5
            timeOnIteration = "1s"
            fork = 2
        }
        else -> error("unknown benchmarkProfile: $benchmarkProfile")
    }
}
