/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.integration.test.sql;

import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.Getter;
import lombok.ToString;

/** A test sql files which contains sql queries. */
@Getter
@ToString
public class TestCase {
  private Path testFile;

  public TestCase(Path testFile) {
    this.testFile = testFile;
  }

  public Path getTestOutputFile() {
    String fileName = testFile.getFileName().toString();
    String outputFileName = fileName + ".out";
    Path parentPath = testFile.getParent();
    if (parentPath == null) {
      return Paths.get(outputFileName);
    }
    return parentPath.resolve(outputFileName);
  }
}
