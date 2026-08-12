plugins {
    `java-library`
    id("com.vanniktech.maven.publish.base")
}

description = "Ordered parallel consumption with bounded processing and safe offset commits"

base {
    archivesName = "jvm-toolbox-consumers"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api("org.apache.kafka:kafka-clients:4.3.1")

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

testing {
    suites {
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter("6.1.2")
            dependencies {
                implementation(project())
            }
        }
    }
}

tasks.named("check") {
    dependsOn("javadoc", "integrationTestClasses")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "jvm-toolbox-consumers"

            pom {
                name = "jvm-toolbox-consumers"
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
