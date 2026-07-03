plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    application
}

group = "io.zugzwang.tessera"
version = "0.1.0-SNAPSHOT"

kotlin {
    jvmToolchain(25)
}

application {
    mainClass = "io.zugzwang.tessera.ApplicationKt"
}

dependencies {
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kafka.clients)
    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    implementation(libs.logback.classic)

    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.redpanda)
}

tasks.test {
    useJUnitPlatform()
}

// End-to-end suite: boots the real Dockerfile image against Redpanda.
// Deliberately not wired into `check` so the local build loop stays fast;
// CI runs it as its own job.
testing {
    suites {
        val e2eTest by registering(JvmTestSuite::class) {
            useJUnitJupiter()
            dependencies {
                implementation(project())
                implementation(libs.kafka.clients)
                implementation(libs.kotlin.test.junit5)
                implementation(libs.testcontainers.junit.jupiter)
                implementation(libs.testcontainers.postgresql)
                implementation(libs.testcontainers.redpanda)
            }
        }
    }
}
