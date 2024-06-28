/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.common;

public class IcebergConstants {
  public static final String CATALOG_BACKEND = "catalog-backend";

  public static final String GRAVITINO_JDBC_USER = "jdbc-user";
  public static final String ICEBERG_JDBC_USER = "jdbc.user";

  public static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
  public static final String ICEBERG_JDBC_PASSWORD = "jdbc.password";
  public static final String ICEBERG_JDBC_INITIALIZE = "jdbc-initialize";

  public static final String GRAVITINO_JDBC_DRIVER = "jdbc-driver";
  public static final String WAREHOUSE = "warehouse";
  public static final String URI = "uri";
  public static final String CATALOG_BACKEND_NAME = "catalog-backend-name";

  public static final String ICEBERG_METRICS_STORE = "metricsStore";
  public static final String ICEBERG_METRICS_STORE_RETAIN_DAYS = "metricsStoreRetainDays";
  public static final String ICEBERG_METRICS_QUEUE_CAPACITY = "metricsQueueCapacity";

  public static final String GRAVITINO_ICEBERG_REST_SERVICE_NAME = "iceberg-rest";

}
