/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":server-common"))
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.databind)
  implementation(libs.metrics.jersey2)

  // As of Java 9 or newer, the javax.activation package (needed by the jetty server) is no longer part of the JDK. It was removed because it was part of the
  // JavaBeans Activation Framework (JAF) which has been removed from Java SE. So we need to add it as a dependency. For more,
  // please see: https://stackoverflow.com/questions/46493613/what-is-the-replacement-for-javax-activation-package-in-java-9
  implementation(libs.sun.activation)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(libs.commons.io)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

fun getGitCommitId(): String {
  var gitCommitId: String
  try {
    val gitFolder = rootDir.path + "/.git/"
    val head = File(gitFolder + "HEAD").readText().split(":")
    val isCommit = head.size == 1
    gitCommitId = if (isCommit) {
      head[0].trim()
    } else {
      val refHead = File(gitFolder + head[1].trim())
      refHead.readText().trim()
    }
  } catch (e: Exception) {
    println("WARN: Unable to get Git commit id : ${e.message}")
    gitCommitId = ""
  }
  return gitCommitId
}

tasks {
  test {
    environment("GRAVITINO_HOME", rootDir.path)
    environment("GRAVITINO_TEST", "true")
  }
}
