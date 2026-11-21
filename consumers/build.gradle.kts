plugins {
    `java-library`
    id("com.vanniktech.maven.publish.base")
}

description = "Ordered parallel consumption with bounded processing and safe offset commits"

base {
    archivesName = "jvm-toolbox-consumers"
}

dependencies {
    api("org.apache.kafka:kafka-clients:4.3.1")
}

testing {
    suites {
        register<JvmTestSuite>("integrationTest") {
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
