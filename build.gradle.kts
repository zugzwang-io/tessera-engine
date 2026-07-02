plugins {
    alias(libs.plugins.kotlin.jvm)
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
    implementation(libs.logback.classic)

    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlin.test.junit5)
}

tasks.test {
    useJUnitPlatform()
}
