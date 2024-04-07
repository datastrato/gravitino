/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkIcebergCatalogIT extends SparkCommonIT {

  protected List<SparkTableInfo.SparkColumnInfo> getIcebergSimpleTableColumn() {
    return Arrays.asList(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkTableInfo.SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkTableInfo.SparkColumnInfo.of("ts", DataTypes.TimestampType, null));
  }

  private String getCreateIcebergSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', ts TIMESTAMP)",
        tableName);
  }

  @Override
  protected String getCatalogName() {
    return "iceberg";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-iceberg";
  }

  @Override
  protected boolean supportsSparkSQLClusteredBy() {
    return false;
  }

  @Override
  protected boolean supportsPartition() {
    return true;
  }

  @Test
  void testCreateIcebergBucketPartitionTable() {
    String tableName = "iceberg_bucket_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (bucket(16, id));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withBucket(16, Collections.singletonList("id"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertTrue(queryResult.size() == 1);
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "id_bucket=4";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergHourPartitionTable() {
    String tableName = "iceberg_hour_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (hours(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withHour(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertTrue(queryResult.size() == 1);
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_hour=12";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergDayPartitionTable() {
    String tableName = "iceberg_day_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (days(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withDay(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertTrue(queryResult.size() == 1);
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_day=2024-01-01";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergMonthPartitionTable() {
    String tableName = "iceberg_month_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (months(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withMonth(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertTrue(queryResult.size() == 1);
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_month=2024-01";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergYearPartitionTable() {
    String tableName = "iceberg_year_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (years(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withYear(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertTrue(queryResult.size() == 1);
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_year=2024";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergTruncatePartitionTable() {
    String tableName = "iceberg_truncate_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (truncate(1, name));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withTruncate(1, Collections.singletonList("name"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertTrue(queryResult.size() == 1);
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "name_trunc=a";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testIcebergFileLevelDeleteOperation() {
    String tableName = "test_delete_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    sql(
        String.format(
            "INSERT INTO %s VALUES (1, '1', 1),(2, '2', 2),(3, '3', 3),(4, '4', 4),(5, '5', 5)",
            tableName));
    List<String> queryResult1 = getTableData(tableName);
    Assertions.assertEquals(5, queryResult1.size());
    Assertions.assertEquals("1,1,1;2,2,2;3,3,3;4,4,4;5,5,5", String.join(";", queryResult1));
    sql(getDeleteSql(tableName, "id < 10"));
    List<String> queryResult2 = getTableData(tableName);
    Assertions.assertEquals(0, queryResult2.size());
  }
}
