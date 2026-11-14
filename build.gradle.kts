// build.gradle.kts — Kotlin/JVM build for the Sentinel Probe SDK.
//
// Mirrors sdk/go's module layout one-to-one under a single Gradle module. Generated proto types
// live under gen/ (gitignored, produced by tools/generate-jvm-sdk.sh) and are wired into the
// main source set here, so there is no protobuf Gradle plugin and no network fetch at build
// time beyond the regular dependency resolution.
//
// JDK 21 LTS is the floor (the Kotlin 2.4.10 / Java 21 toolchain that ships via devbox).
//
// Dependency resolution is locked and verified (#494). Every configuration, including the
// plugin classpath below, resolves only what gradle.lockfile / buildscript-gradle.lockfile
// record, and STRICT mode also fails a locked configuration that has no lock state at all.
// STRICT has to be set twice: `buildscript.dependencyLocking` is a separate extension from
// the project's, so the project block further down does not reach the plugin classpath.
// gradle/verification-metadata.xml checks the SHA-256 of every artifact, which also covers
// detached configurations that locking cannot see. Regenerate all three with
// `just lock-sdk-jvm` (networked); tools/test-gradle-lock-drift.sh proves on the gate that
// an unlocked dependency, a missing plugin-classpath lock file and a corrupted checksum
// each fail.

// Must precede `plugins {}`: it locks the plugin classpath, which the plugins DSL resolves
// through this configuration (plugin markers included).
buildscript {
    dependencyLocking {
        lockMode.set(LockMode.STRICT)
    }
    configurations.classpath {
        resolutionStrategy.activateDependencyLocking()
    }
}

plugins {
    kotlin("jvm") version "2.4.10"
    id("org.jlleitschuh.gradle.ktlint") version "12.3.0"
    id("io.gitlab.arturbosch.detekt") version "1.23.8"
}

repositories {
    mavenCentral()
}

dependencyLocking {
    lockAllConfigurations()
    lockMode.set(LockMode.STRICT)
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
    // ADR-0002: OTel is an adapter, not the substrate. The Probe-wire addendum exports
    // OTLP/HTTP protobuf. The proto jar is the v1.5.0 messages the ingest decoder uses.
    implementation("io.opentelemetry:opentelemetry-sdk:1.47.0")
    implementation("io.opentelemetry:opentelemetry-api:1.47.0")
    implementation("io.opentelemetry.proto:opentelemetry-proto:1.5.0-alpha")

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
    systemProperty("sentinel.repository.root", rootProject.projectDir.parentFile.parentFile.absolutePath)
    val peripheryBin = System.getenv("SENTINEL_PERIPHERY_BIN")
    if (!peripheryBin.isNullOrBlank()) {
        systemProperty("sentinel.periphery.bin", peripheryBin)
    }
}

ktlint {
    version.set("1.7.1")
}

detekt {
    buildUponDefaultConfig = true
    allRules = false
    config.setFrom(files("detekt.yml"))
}
