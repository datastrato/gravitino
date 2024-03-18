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
  maven {
    name = "remoteVirtual"
    url = uri("https://pkgs.d.xiaomi.net:443/artifactory/maven-remote-virtual/")
  }
  maven {
    name = "releaseVirtual"
    url = uri("https://pkgs.d.xiaomi.net:443/artifactory/maven-release-virtual/")
  }
  maven {
    name = "snapshotVirtual"
    url = uri("https://pkgs.d.xiaomi.net:443/artifactory/maven-snapshot-virtual/")
  }
}

val scalaVersion: String = project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val sparkVersion: String = libs.versions.spark.get()
val icebergVersion: String = libs.versions.iceberg.get()
val kyuubiVersion: String = libs.versions.kyuubi.get()
val scalaJava8CompatVersion: String = libs.versions.scala.java.compat.get()

dependencies {
  implementation(project(":api"))
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(project(":common"))
  implementation(libs.bundles.log4j)
  implementation(libs.guava)
  implementation("org.apache.iceberg:iceberg-spark-runtime-3.4_$scalaVersion:$icebergVersion")
  implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  implementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
  implementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
  implementation("org.scala-lang.modules:scala-java8-compat_$scalaVersion:$scalaJava8CompatVersion")

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
