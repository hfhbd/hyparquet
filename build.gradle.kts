plugins {
    kotlin("jvm") version "2.3.0-Beta1"
    kotlin("plugin.serialization") version "2.3.0-Beta1"
}

kotlin {
    jvmToolchain(8)
    compilerOptions {
        optIn.add("kotlin.time.ExperimentalTime")
    }
}

dependencies {
    implementation(libs.serialization.json)
    implementation(libs.io.core)

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

java {
    withSourcesJar()
    withJavadocJar()
}

sourceSets.main {
    resources.srcDir(file("test/files"))
}
