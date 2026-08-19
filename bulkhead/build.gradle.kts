plugins {
    `java-library`
    id("com.vanniktech.maven.publish.base")
    id("me.champeau.jmh")
}

description = "Bounded concurrent calls with explicit overload behavior"

base {
    archivesName = "jvm-toolbox-bulkhead"
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
    dependsOn("javadoc", "jmhClasses", verifyRuntimeClasspath)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "jvm-toolbox-bulkhead"

            pom {
                name = "jvm-toolbox-bulkhead"
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
            timeUnit = "ns"
        }
        "allocation" -> {
            warmupIterations = 2
            warmup = "500ms"
            iterations = 3
            timeOnIteration = "500ms"
            fork = 1
            benchmarkMode = listOf("avgt")
            timeUnit = "ns"
            profilers = listOf("gc")
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
