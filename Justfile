# Tasks for this published mirror of the Sentinel JVM (Kotlin) Probe SDK (#160, ADR-0032).
#
# Run inside the Devbox shell: `devbox install` once, then `devbox shell`. These
# recipes reuse the canonical gate names of Sentinel's source repository, scoped to
# this one language. The generated Java proto types under gen/ are committed here and
# wired as a source root by build.gradle.kts, so no recipe generates anything;
# regenerating is described in CONTRIBUTING.md.

build:
    ./gradlew build

test:
    ./gradlew test

lint:
    ./gradlew detekt

fmt-check:
    ./gradlew ktlintCheck
