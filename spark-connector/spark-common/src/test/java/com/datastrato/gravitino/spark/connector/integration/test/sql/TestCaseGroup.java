/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.integration.test.sql;

import java.nio.file.Path;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * A group of test SQL files in same directory which belongs to one catalog and may contain
 * prepare.sql to init or cleanup.sql to clean.
 */
@Getter
public class TestCaseGroup {

  List<TestCase> testCases;
  @Nullable Path prepareFile;
  @Nullable Path cleanupFile;
  CatalogType catalogType;

  public TestCaseGroup(
      List<TestCase> testCases, Path prepareFile, Path cleanupFile, CatalogType catalogType) {
    this.testCases = testCases;
    this.prepareFile = prepareFile;
    this.cleanupFile = cleanupFile;
    this.catalogType = catalogType;
  }
}
