/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.integration.test;

import com.datastrato.gravitino.iceberg.common.IcebergCatalogBackend;
import com.datastrato.gravitino.iceberg.integration.test.util.IcebergRESTServerManager;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * <p>Referred from spark/v3.4/spark/src/test/java/org/apache/iceberg/spark/SparkTestBase.java
 */

@SuppressWarnings("FormatStringAnnotation")
public abstract class IcebergRESTServiceBaseIT {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServiceBaseIT.class);
  private SparkSession sparkSession;
  protected IcebergCatalogBackend catalogType = IcebergCatalogBackend.MEMORY;
  private IcebergRESTServerManager icebergRESTServerManager;

  @BeforeAll
  void initIcebergTestEnv() throws Exception {
    // Start Gravitino docker container
    initEnv();
    // Start Gravitino server
    this.icebergRESTServerManager = IcebergRESTServerManager.create();
    // Inject Iceberg REST service config to gravitino.conf
    registerIcebergCatalogConfig();
    icebergRESTServerManager.startIcebergRESTServer();
    // Start Spark session
    initSparkEnv();
    LOG.info("Gravitino and Spark env started,{}", catalogType);
  }

  @AfterAll
  void stopIcebergTestEnv() throws Exception {
    stopSparkEnv();
    icebergRESTServerManager.stopIcebergRESTServer();
    LOG.info("Gravitino and Spark env stopped,{}", catalogType);
  }

  boolean catalogTypeNotMemory() {
    return !catalogType.equals(IcebergCatalogBackend.MEMORY);
  }

  abstract void initEnv();

  abstract Map<String, String> getCatalogConfig();

  private void registerIcebergCatalogConfig() {
    Map<String, String> icebergConfigs = getCatalogConfig();
    icebergRESTServerManager.registerCustomConfigs(icebergConfigs);
    LOG.info("Iceberg REST service config registered, {}", StringUtils.join(icebergConfigs));
  }

  private int getServerPort() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            icebergRESTServerManager.getServerConfig(),
            JettyServerConfig.GRAVITINO_SERVER_CONFIG_PREFIX);
    return jettyServerConfig.getHttpPort();
  }

  private void initSparkEnv() {
    int port = getServerPort();
    LOG.info("Iceberg REST server port:{}", port);
    String IcebergRESTUri = String.format("http://127.0.0.1:%d/iceberg/", port);
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.rest.type", "rest")
            .config("spark.sql.catalog.rest.uri", IcebergRESTUri)
            // drop Iceberg table purge may hang in spark local mode
            .config("spark.locality.wait.node", "0")
            .getOrCreate();
  }

  private void stopSparkEnv() {
    if (sparkSession != null) {
      sparkSession.close();
      sparkSession = null;
    }
  }

  @FormatMethod
  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = sparkSession.sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return ImmutableList.of();
    }
    return rowsToJava(rows);
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }
              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              }
              return value;
            })
        .toArray(Object[]::new);
  }

  /** check whether all child map content is in parent map */
  protected void checkMapContains(Map<String, String> child, Map<String, String> parent) {
    child.forEach(
        (k, v) -> {
          Assertions.assertTrue(parent.containsKey(k));
          Assertions.assertEquals(v, parent.get(k));
        });
  }

  /** mainly used to debug */
  protected void printObjects(List<Object[]> objects) {
    objects.stream()
        .forEach(
            row -> {
              String oneRow =
                  Arrays.stream(row).map(o -> String.valueOf(o)).collect(Collectors.joining(","));
              LOG.warn(oneRow);
            });
  }

  protected Map<String, String> getTableInfo(String tableName) {
    return convertToStringMap(sql("desc table extended " + tableName));
  }

  protected List<String> getTableColumns(String tableName) {
    List<Object[]> objects = sql("desc table extended " + tableName);
    List<String> columns = new ArrayList<>();
    objects.stream()
        .anyMatch(
            row -> {
              String columName = (String) row[0];
              if (StringUtils.isNoneBlank(columName)) {
                columns.add(columName);
                return false;
              }
              return true;
            });
    return columns;
  }

  protected Set<String> convertToStringSet(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toSet());
  }

  protected List<String> convertToStringList(List<Object[]> objects, int index) {
    return objects.stream().map(row -> String.valueOf(row[index])).collect(Collectors.toList());
  }

  protected Map<String, String> convertToStringMap(List<Object[]> objects) {
    return objects.stream()
        .collect(
            Collectors.toMap(
                row -> String.valueOf(row[0]),
                row -> String.valueOf(row[1]),
                (oldValue, newValue) -> oldValue));
  }
}
