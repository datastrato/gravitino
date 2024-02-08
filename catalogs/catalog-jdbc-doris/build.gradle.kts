/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-jdbc-doris"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":catalogs:catalog-jdbc-common"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(libs.bundles.log4j)
  implementation(libs.commons.collections4)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jsqlparser)

  testImplementation(project(":catalogs:catalog-jdbc-common"))
  testImplementation(libs.mysql.driver)
  testImplementation(libs.guava)
  testImplementation(libs.commons.lang3)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs_all")
  }
  val copyCatalogLibs by registering(Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs_all", "build/libs")
    into("$rootDir/distribution/package/catalogs/jdbc-doris/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/jdbc-doris/conf")

    include("jdbc-doris.conf")

    exclude { details ->
      details.file.isDirectory()
    }
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyCatalogLibs, copyCatalogConfig)
  }
}
