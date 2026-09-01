// build.gradle.kts — Kotlin/JVM build for the Sentinel Probe SDK.
//
// Mirrors sdk/go's module layout one-to-one under a single Gradle module. Generated proto types
// live under gen/ (gitignored, produced by tools/generate-jvm-sdk.sh) and are wired into the
// main source set here, so there is no protobuf Gradle plugin and no network fetch at build
// time beyond the regular dependency resolution.
//
// JDK 21 LTS is the floor (the Kotlin 2.4.10 / Java 21 toolchain that ships via devbox).

plugins {
    kotlin("jvm") version "2.4.10"
    id("org.jlleitschuh.gradle.ktlint") version "12.3.0"
    id("io.gitlab.arturbosch.detekt") version "1.23.8"
}

repositories {
    mavenCentral()
}

// Generated Java proto types are a source root, not a dependency: tools/generate-jvm-sdk.sh
// materialises gen/ before any Gradle task runs, exactly as sdk/go expects sdk/go/gen/.
sourceSets {
    main {
        java.srcDir("gen")
    }
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    // Matches BSR buf.build/protocolbuffers/java:v33.4 runtime requirements.
    implementation("com.google.protobuf:protobuf-java:4.33.4")
    // ADR-0002: OTel is an adapter, not the substrate. The host owns export.
    implementation("io.opentelemetry:opentelemetry-sdk:1.47.0")
    implementation("io.opentelemetry:opentelemetry-api:1.47.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.13.4")
    testImplementation("io.kotest:kotest-property:5.9.1")
    // Kotest property on the JVM pulls its assertion API; keep it test-scoped.
    testImplementation("io.kotest:kotest-assertions-core:5.9.1")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    testImplementation(kotlin("test"))
    // a running tracer, so SpanToEvent is tested without exporting anywhere.
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:1.47.0")
}

tasks.test {
    useJUnitPlatform()
    // Hermetic tests (ADR-0019): pin deterministic behaviour, no wall-clock reliance.
    testLogging {
        events("passed", "skipped", "failed")
    }
    systemProperty("sentinel.repository.root", rootProject.projectDir.absolutePath)
}

ktlint {
    version.set("1.7.1")
}

detekt {
    buildUponDefaultConfig = true
    allRules = false
    config.setFrom(files("detekt.yml"))
}
