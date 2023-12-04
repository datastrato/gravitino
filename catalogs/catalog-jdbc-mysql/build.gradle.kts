/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
description = "catalog-jdbc-mysql"

plugins {
    `maven-publish`
    id("java")
    id("idea")
}

dependencies {
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":api"))
  implementation(project(":catalogs:catalog-jdbc-common"))
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.commons.collections4)
  implementation(libs.jsqlparser)
}

tasks {
  val copyDepends by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }
  val copyCatalogLibs by registering(Copy::class) {
    dependsOn(copyDepends, "build")
    from("build/libs")
    into("$rootDir/distribution/package/catalogs/jdbc-mysql/libs")
  }

  val copyCatalogConfig by registering(Copy::class) {
    from("src/main/resources")
    into("$rootDir/distribution/package/catalogs/jdbc-mysql/conf")

    include("jdbc-mysql.conf")

    exclude { details ->
      details.file.isDirectory()
    }
  }

  val copyLibAndConfig by registering(Copy::class) {
    dependsOn(copyCatalogLibs, copyCatalogConfig)
  }
}
