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
}
