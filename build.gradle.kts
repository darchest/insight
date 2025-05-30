val ktorVersion = "2.3.12"
val apachePoiVersion = "5.3.0"

plugins {
    kotlin("jvm") version "2.0.10"
    `java-library`
}

group = "org.darchest"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.commons:commons-dbcp2:2.9.0")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("org.slf4j:slf4j-api:2.0.3")
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.0")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(8)
}