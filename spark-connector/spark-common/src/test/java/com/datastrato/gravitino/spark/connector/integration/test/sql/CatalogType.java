/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.integration.test.sql;

public enum CatalogType {
  HIVE,
  ICEBERG,
  UNKNOWN;

  public static CatalogType fromString(String str) {
    if (str == null) {
      return UNKNOWN;
    }
    switch (str.toLowerCase()) {
      case "hive":
        return HIVE;
      case "lakehouse-iceberg":
        return ICEBERG;
      default:
        return UNKNOWN;
    }
  }

  public static CatalogType merge(CatalogType parentCatalogType, CatalogType childCatalogType) {
    if (parentCatalogType.equals(UNKNOWN)) {
      return childCatalogType;
    } else {
      return parentCatalogType;
    }
  }
}
