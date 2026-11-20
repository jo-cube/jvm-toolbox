plugins {
    id("com.vanniktech.maven.publish.base") version "0.37.0" apply false
    id("me.champeau.jmh") version "0.7.3" apply false
}

val releaseVersion = providers.gradleProperty("releaseVersion").orElse("0.0.0-SNAPSHOT")

subprojects {
    group = "io.github.jo-cube"
    version = releaseVersion.get()

    repositories {
        mavenCentral()
    }

    plugins.withType<JavaPlugin> {
        extensions.configure<JavaPluginExtension> {
            toolchain.languageVersion = JavaLanguageVersion.of(25)
            withSourcesJar()
            withJavadocJar()
        }

        tasks.withType<JavaCompile>().configureEach {
            options.release = 25
            options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
        }

        tasks.withType<Javadoc>().configureEach {
            (options as StandardJavadocDocletOptions).addBooleanOption("Werror", true)
        }

        tasks.withType<Jar>().configureEach {
            from(rootProject.file("LICENSE")) {
                into("META-INF")
            }
        }

        extensions.configure<TestingExtension> {
            suites.withType<JvmTestSuite>().configureEach {
                useJUnitJupiter("6.1.2")
            }
        }
    }
}
