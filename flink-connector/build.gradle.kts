/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
}

repositories {
  mavenCentral()
}

val flinkVrsion: String = libs.versions.flink.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(project(":common"))
  implementation(libs.bundles.log4j)
  implementation(libs.guava)
  implementation("org.apache.flink:flink-table-common:$flinkVrsion")
  implementation("org.apache.flink:flink-table-api-java:$flinkVrsion")
  implementation("org.apache.flink:flink-connector-hive_2.12:$flinkVrsion")

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
