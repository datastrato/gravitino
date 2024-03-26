/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public abstract class SparkCommonIT extends SparkEnvIT {

  // To generate test data for write&read table.
  private static final Map<DataType, String> typeConstant =
      ImmutableMap.of(
          DataTypes.IntegerType,
          "2",
          DataTypes.StringType,
          "'gravitino_it_test'",
          DataTypes.createArrayType(DataTypes.IntegerType),
          "array(1, 2, 3)",
          DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
          "map('a', 1)",
          DataTypes.createStructType(
              Arrays.asList(
                  DataTypes.createStructField("col1", DataTypes.IntegerType, true),
                  DataTypes.createStructField("col2", DataTypes.StringType, true))),
          "struct(1, 'a')");

  // To generate test data for update table.
  private static final Map<DataType, String> typeNewConstant =
      ImmutableMap.of(
          DataTypes.IntegerType,
          "2",
          DataTypes.StringType,
          "'gravitino_it_test_new'",
          DataTypes.createArrayType(DataTypes.IntegerType),
          "array(4, 5, 6)",
          DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
          "map('b', 2)",
          DataTypes.createStructType(
              Arrays.asList(
                  DataTypes.createStructField("col1", DataTypes.IntegerType, true),
                  DataTypes.createStructField("col2", DataTypes.StringType, true))),
          "struct(2, 'b')");

  private static String getInsertWithoutPartitionSql(String tableName, String values) {
    return String.format("INSERT INTO %s VALUES (%s)", tableName, values);
  }

  private static String getInsertWithPartitionSql(
      String tableName, String partitionString, String values) {
    return String.format(
        "INSERT OVERWRITE %s PARTITION (%s) VALUES (%s)", tableName, partitionString, values);
  }

  private static String getUpdateTableSql(String tableName, String setClause, String whereClause) {
    return String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
  }

  private static String getRowLevelUpdateTableSql(
      String targetTableName, String selectClause, String sourceTableName, String onClause) {
    return String.format(
        "MERGE INTO %s "
            + "USING (SELECT %s) %s "
            + "ON %s "
            + "WHEN MATCHED THEN UPDATE SET * "
            + "WHEN NOT MATCHED THEN INSERT *",
        targetTableName, selectClause, sourceTableName, onClause);
  }

  private static String getRowLevelDeleteTableSql(
      String targetTableName, String selectClause, String sourceTableName, String onClause) {
    return String.format(
        "MERGE INTO %s "
            + "USING (SELECT %s) %s "
            + "ON %s "
            + "WHEN MATCHED THEN DELETE "
            + "WHEN NOT MATCHED THEN INSERT *",
        targetTableName, selectClause, sourceTableName, onClause);
  }

  // Whether supports [CLUSTERED BY col_name3 SORTED BY col_name INTO num_buckets BUCKETS]
  protected abstract boolean supportsSparkSQLClusteredBy();

  // Use a custom database not the original default database because SparkCommonIT couldn't
  // read&write data to tables in default database. The main reason is default database location is
  // determined by `hive.metastore.warehouse.dir` in hive-site.xml which is local HDFS address
  // not real HDFS address. The location of tables created under default database is like
  // hdfs://localhost:9000/xxx which couldn't read write data from SparkCommonIT. Will use default
  // database after spark connector support Alter database xx set location command.
  @BeforeAll
  void initDefaultDatabase() {
    sql("USE " + getCatalogName());
    createDatabaseIfNotExists(getDefaultDatabase());
  }

  @BeforeEach
  void init() {
    sql("USE " + getCatalogName());
    sql("USE " + getDefaultDatabase());
  }

  @AfterAll
  void cleanUp() {
    sql("USE " + getCatalogName());
    getDatabases()
        .forEach(
            databaseName -> sql(String.format("DROP DATABASE IF EXISTS %s CASCADE", databaseName)));
  }

  @Test
  void testLoadCatalogs() {
    Set<String> catalogs = getCatalogs();
    Assertions.assertTrue(catalogs.contains(getCatalogName()));
  }

  @Test
  void testDropSchema() {
    String testDatabaseName = "t_drop";
    Set<String> databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    sql("CREATE DATABASE " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertTrue(databases.contains(testDatabaseName));

    sql("DROP DATABASE " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DROP DATABASE notExists"));
  }

  @Test
  void testCreateSimpleTable() {
    String tableName = "simple_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withComment(null);
    checker.check(tableInfo);

    checkTableReadWrite(tableInfo);
  }

  @Test
  void testCreateTableWithDatabase() {
    // test db.table as table identifier
    String databaseName = "db1";
    String tableName = "table1";
    createDatabaseIfNotExists(databaseName);
    String tableIdentifier = String.join(".", databaseName, tableName);

    createSimpleTable(tableIdentifier);
    SparkTableInfo tableInfo = getTableInfo(tableIdentifier);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create().withName(tableName).withColumns(getSimpleTableColumn());
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);

    // use db then create table with table name
    databaseName = "db2";
    tableName = "table2";
    createDatabaseIfNotExists(databaseName);

    sql("USE " + databaseName);
    createSimpleTable(tableName);
    tableInfo = getTableInfo(tableName);
    checker =
        SparkTableInfoChecker.create().withName(tableName).withColumns(getSimpleTableColumn());
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  void testCreateTableWithComment() {
    String tableName = "comment_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName);
    String tableComment = "tableComment";
    createTableSql = String.format("%s COMMENT '%s'", createTableSql, tableComment);
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withComment(tableComment);
    checker.check(tableInfo);

    checkTableReadWrite(tableInfo);
  }

  @Test
  void testDropTable() {
    String tableName = "drop_table";
    createSimpleTable(tableName);
    Assertions.assertEquals(true, tableExists(tableName));

    dropTableIfExists(tableName);
    Assertions.assertEquals(false, tableExists(tableName));

    Assertions.assertThrowsExactly(NoSuchTableException.class, () -> sql("DROP TABLE not_exists"));
  }

  @Test
  void testRenameTable() {
    String tableName = "rename1";
    String newTableName = "rename2";
    dropTableIfExists(tableName);
    dropTableIfExists(newTableName);

    createSimpleTable(tableName);
    Assertions.assertTrue(tableExists(tableName));
    Assertions.assertFalse(tableExists(newTableName));

    sql(String.format("ALTER TABLE %s RENAME TO %s", tableName, newTableName));
    Assertions.assertTrue(tableExists(newTableName));
    Assertions.assertFalse(tableExists(tableName));

    // rename to an existing table
    createSimpleTable(tableName);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> sql(String.format("ALTER TABLE %s RENAME TO %s", tableName, newTableName)));

    // rename a not existing tables
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("ALTER TABLE not_exists1 RENAME TO not_exist2"));
  }

  @Test
  void testListTable() {
    String table1 = "list1";
    String table2 = "list2";
    createSimpleTable(table1);
    createSimpleTable(table2);
    Set<String> tables = listTableNames();
    Assertions.assertTrue(tables.contains(table1));
    Assertions.assertTrue(tables.contains(table2));

    // show tables from not current db
    String database = "db_list";
    String table3 = "list3";
    String table4 = "list4";
    createDatabaseIfNotExists(database);
    createSimpleTable(String.join(".", database, table3));
    createSimpleTable(String.join(".", database, table4));
    tables = listTableNames(database);

    Assertions.assertTrue(tables.contains(table3));
    Assertions.assertTrue(tables.contains(table4));

    Assertions.assertThrows(NoSuchNamespaceException.class, () -> listTableNames("not_exists_db"));
  }

  @Test
  void testAlterTableSetAndRemoveProperty() {
    String tableName = "test_property";
    dropTableIfExists(tableName);

    createSimpleTable(tableName);
    sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES('key1'='value1', 'key2'='value2')", tableName));
    Map<String, String> oldProperties = getTableInfo(tableName).getTableProperties();
    Assertions.assertTrue(oldProperties.containsKey("key1") && oldProperties.containsKey("key2"));

    sql(String.format("ALTER TABLE %s UNSET TBLPROPERTIES('key1')", tableName));
    Map<String, String> newProperties = getTableInfo(tableName).getTableProperties();
    Assertions.assertFalse(newProperties.containsKey("key1"));
    Assertions.assertTrue(newProperties.containsKey("key2"));
  }

  @Test
  void testAlterTableAddAndDeleteColumn() {
    String tableName = "test_column";
    dropTableIfExists(tableName);

    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();

    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 string)", tableName));
    ArrayList<SparkColumnInfo> addColumns = new ArrayList<>(simpleTableColumns);
    addColumns.add(SparkColumnInfo.of("col1", DataTypes.StringType, null));
    checkTableColumns(tableName, addColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S DROP COLUMNS (col1)", tableName));
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));
  }

  @Test
  void testAlterTableUpdateColumnType() {
    String tableName = "test_column_type";
    dropTableIfExists(tableName);

    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();

    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 int)", tableName));
    sql(String.format("ALTER TABLE %S CHANGE COLUMN col1 col1 bigint", tableName));
    ArrayList<SparkColumnInfo> updateColumns = new ArrayList<>(simpleTableColumns);
    updateColumns.add(SparkColumnInfo.of("col1", DataTypes.LongType, null));
    checkTableColumns(tableName, updateColumns, getTableInfo(tableName));
  }

  @Test
  void testAlterTableRenameColumn() {
    String tableName = "test_rename_column";
    dropTableIfExists(tableName);
    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();
    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    String oldColumnName = "col1";
    String newColumnName = "col2";

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 int)", tableName));
    sql(
        String.format(
            "ALTER TABLE %S RENAME COLUMN %S TO %S", tableName, oldColumnName, newColumnName));
    ArrayList<SparkColumnInfo> renameColumns = new ArrayList<>(simpleTableColumns);
    renameColumns.add(SparkColumnInfo.of(newColumnName, DataTypes.IntegerType, null));
    checkTableColumns(tableName, renameColumns, getTableInfo(tableName));
  }

  @Test
  void testUpdateColumnPosition() {
    String tableName = "test_column_position";
    dropTableIfExists(tableName);

    List<SparkColumnInfo> simpleTableColumns =
        Arrays.asList(
            SparkColumnInfo.of("id", DataTypes.StringType, ""),
            SparkColumnInfo.of("name", DataTypes.StringType, ""),
            SparkColumnInfo.of("age", DataTypes.StringType, ""));

    sql(
        String.format(
            "CREATE TABLE %s (id STRING COMMENT '', name STRING COMMENT '', age STRING COMMENT '')",
            tableName));
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 STRING COMMENT '')", tableName));
    List<SparkColumnInfo> updateColumnPositionCol1 = new ArrayList<>(simpleTableColumns);
    updateColumnPositionCol1.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    checkTableColumns(tableName, updateColumnPositionCol1, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S CHANGE COLUMN col1 col1 STRING FIRST", tableName));
    List<SparkColumnInfo> updateColumnPositionFirst = new ArrayList<>();
    updateColumnPositionFirst.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    updateColumnPositionFirst.addAll(simpleTableColumns);
    checkTableColumns(tableName, updateColumnPositionFirst, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col2 STRING COMMENT '')", tableName));
    List<SparkColumnInfo> updateColumnPositionCol2 = new ArrayList<>();
    updateColumnPositionCol2.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    updateColumnPositionCol2.addAll(simpleTableColumns);
    updateColumnPositionCol2.add(SparkColumnInfo.of("col2", DataTypes.StringType, ""));
    checkTableColumns(tableName, updateColumnPositionCol2, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S CHANGE COLUMN col2 col2 STRING AFTER col1", tableName));
    List<SparkColumnInfo> updateColumnPositionAfter = new ArrayList<>();
    updateColumnPositionAfter.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    updateColumnPositionAfter.add(SparkColumnInfo.of("col2", DataTypes.StringType, ""));
    updateColumnPositionAfter.addAll(simpleTableColumns);
    checkTableColumns(tableName, updateColumnPositionAfter, getTableInfo(tableName));
  }

  @Test
  void testAlterTableUpdateColumnComment() {
    String tableName = "test_update_column_comment";
    dropTableIfExists(tableName);
    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();
    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    String oldColumnComment = "col1_comment";
    String newColumnComment = "col1_new_comment";

    sql(
        String.format(
            "ALTER TABLE %S ADD COLUMNS (col1 int comment '%s')", tableName, oldColumnComment));
    sql(
        String.format(
            "ALTER TABLE %S CHANGE COLUMN col1 col1 int comment '%s'",
            tableName, newColumnComment));
    ArrayList<SparkColumnInfo> updateCommentColumns = new ArrayList<>(simpleTableColumns);
    updateCommentColumns.add(SparkColumnInfo.of("col1", DataTypes.IntegerType, newColumnComment));
    checkTableColumns(tableName, updateCommentColumns, getTableInfo(tableName));
  }

  @Test
  void testComplexType() {
    String tableName = "complex_type_table";
    dropTableIfExists(tableName);

    sql(
        String.format(
            "CREATE TABLE %s (col1 ARRAY<INT> COMMENT 'array', col2 MAP<STRING, INT> COMMENT 'map', col3 STRUCT<col1: INT, col2: STRING> COMMENT 'struct')",
            tableName));
    SparkTableInfo tableInfo = getTableInfo(tableName);
    List<SparkColumnInfo> expectedSparkInfo =
        Arrays.asList(
            SparkColumnInfo.of("col1", DataTypes.createArrayType(DataTypes.IntegerType), "array"),
            SparkColumnInfo.of(
                "col2",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
                "map"),
            SparkColumnInfo.of(
                "col3",
                DataTypes.createStructType(
                    Arrays.asList(
                        DataTypes.createStructField("col1", DataTypes.IntegerType, true),
                        DataTypes.createStructField("col2", DataTypes.StringType, true))),
                "struct"));
    checkTableColumns(tableName, expectedSparkInfo, tableInfo);

    checkTableReadWrite(tableInfo);
  }

  @Test
  void testCreateDatasourceFormatPartitionTable() {
    String tableName = "datasource_partition_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + "USING PARQUET PARTITIONED BY (name, age)";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withIdentifyPartition(Arrays.asList("name", "age"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
    checkPartitionDirExists(tableInfo);
  }

  @Test
  @EnabledIf("supportsSparkSQLClusteredBy")
  void testCreateBucketTable() {
    String tableName = "bucket_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + "CLUSTERED BY (id, name) INTO 4 buckets;";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withBucket(4, Arrays.asList("id", "name"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  @EnabledIf("supportsSparkSQLClusteredBy")
  void testCreateSortBucketTable() {
    String tableName = "sort_bucket_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL =
        createTableSQL + "CLUSTERED BY (id, name) SORTED BY (name, id) INTO 4 buckets;";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withBucket(4, Arrays.asList("id", "name"), Arrays.asList("name", "id"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  protected void checkPartitionDirExists(SparkTableInfo table) {
    Assertions.assertTrue(table.isPartitionTable(), "Not a partition table");
    String tableLocation = table.getTableLocation();
    String partitionExpression = getPartitionExpression(table, "/").replace("'", "");
    Path partitionPath = new Path(tableLocation, partitionExpression);
    checkDirExists(partitionPath);
  }

  protected void checkDirExists(Path dir) {
    try {
      Assertions.assertTrue(hdfs.exists(dir), "HDFS directory not exists," + dir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void checkTableReadWrite(SparkTableInfo table) {
    String name = table.getTableIdentifier();
    boolean isPartitionTable = table.isPartitionTable();
    String insertValues =
        table.getUnPartitionedColumns().stream()
            .map(columnInfo -> typeConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    String insertDataSQL = "";
    if (isPartitionTable) {
      String partitionExpressions = getPartitionExpression(table, ",");
      insertDataSQL = getInsertWithPartitionSql(name, partitionExpressions, insertValues);
    } else {
      insertDataSQL = getInsertWithoutPartitionSql(name, insertValues);
    }
    sql(insertDataSQL);

    // do something to match the query result:
    // 1. remove "'" from values, such as 'a' is trans to a
    // 2. remove "array" from values, such as array(1, 2, 3) is trans to [1, 2, 3]
    // 3. remove "map" from values, such as map('a', 1, 'b', 2) is trans to {a=1, b=2}
    // 4. remove "struct" from values, such as struct(1, 'a') is trans to 1,a
    String checkValues =
        table.getColumns().stream()
            .map(columnInfo -> typeConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .map(
                s -> {
                  String tmp = org.apache.commons.lang3.StringUtils.remove(s, "'");
                  if (org.apache.commons.lang3.StringUtils.isEmpty(tmp)) {
                    return tmp;
                  } else if (tmp.startsWith("array")) {
                    return tmp.replace("array", "").replace("(", "[").replace(")", "]");
                  } else if (tmp.startsWith("map")) {
                    return tmp.replace("map", "")
                        .replace("(", "{")
                        .replace(")", "}")
                        .replace(", ", "=");
                  } else if (tmp.startsWith("struct")) {
                    return tmp.replace("struct", "")
                        .replace("(", "")
                        .replace(")", "")
                        .replace(", ", ",");
                  }
                  return tmp;
                })
            .collect(Collectors.joining(","));

    List<String> queryResult = getTableData(name);
    Assertions.assertTrue(
        queryResult.size() == 1, "Should just one row, table content: " + queryResult);
    Assertions.assertEquals(checkValues, queryResult.get(0));
  }

  protected void checkTableReadAndUpdate(SparkTableInfo table) {
    String name = table.getTableIdentifier();
    checkTableReadWrite(table);

    String updatedValues =
        table.getColumns().stream()
            .map(
                columnInfo ->
                    String.format(
                        "%s = %s", columnInfo.getName(), typeNewConstant.get(columnInfo.getType())))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    sql(getUpdateTableSql(name, updatedValues, "1 = 1"));

    // do something to match the query result:
    // 1. remove "'" from values, such as 'a' is trans to a
    // 2. remove "array" from values, such as array(1, 2, 3) is trans to [1, 2, 3]
    // 3. remove "map" from values, such as map('a', 1, 'b', 2) is trans to {a=1, b=2}
    // 4. remove "struct" from values, such as struct(1, 'a') is trans to 1,a
    String checkValues =
        table.getColumns().stream()
            .map(columnInfo -> typeNewConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .map(
                s -> {
                  String tmp = org.apache.commons.lang3.StringUtils.remove(s, "'");
                  if (org.apache.commons.lang3.StringUtils.isEmpty(tmp)) {
                    return tmp;
                  } else if (tmp.startsWith("array")) {
                    return tmp.replace("array", "").replace("(", "[").replace(")", "]");
                  } else if (tmp.startsWith("map")) {
                    return tmp.replace("map", "")
                        .replace("(", "{")
                        .replace(")", "}")
                        .replace(", ", "=");
                  } else if (tmp.startsWith("struct")) {
                    return tmp.replace("struct", "")
                        .replace("(", "")
                        .replace(")", "")
                        .replace(", ", ",");
                  }
                  return tmp;
                })
            .collect(Collectors.joining(","));

    List<String> queryResult =
        sql(getSelectAllSql(name)).stream()
            .map(
                line ->
                    Arrays.stream(line)
                        .map(
                            item -> {
                              if (item instanceof Object[]) {
                                return Arrays.stream((Object[]) item)
                                    .map(Object::toString)
                                    .collect(Collectors.joining(","));
                              } else {
                                return item.toString();
                              }
                            })
                        .collect(Collectors.joining(",")))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        1, queryResult.size(), "Should just one row, table content: " + queryResult);
    Assertions.assertEquals(checkValues, queryResult.get(0));
  }

  protected void checkTableRowLevelUpdate(SparkTableInfo table) {
    String name = table.getTableIdentifier();
    checkTableReadWrite(table);

    String sourceTableName = "source_table";

    String selectClause =
        table.getColumns().stream()
            .map(
                columnInfo ->
                    String.format(
                        "%s as %s",
                        typeNewConstant.get(columnInfo.getType()), columnInfo.getName()))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    List<SparkColumnInfo> columns = table.getColumns();
    Preconditions.checkArgument(columns.size() > 0, "columns should not be empty");
    SparkColumnInfo onColumn = columns.get(0);

    String onClause =
        String.format(
            "%s.%s = %s.%s", name, onColumn.getName(), sourceTableName, onColumn.getName());

    sql(getRowLevelUpdateTableSql(name, sourceTableName, selectClause, onClause));

    // do something to match the query result:
    // 1. remove "'" from values, such as 'a' is trans to a
    // 2. remove "array" from values, such as array(1, 2, 3) is trans to [1, 2, 3]
    // 3. remove "map" from values, such as map('a', 1, 'b', 2) is trans to {a=1, b=2}
    // 4. remove "struct" from values, such as struct(1, 'a') is trans to 1,a
    String checkValues =
        table.getColumns().stream()
            .map(columnInfo -> typeNewConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .map(
                s -> {
                  String tmp = org.apache.commons.lang3.StringUtils.remove(s, "'");
                  if (org.apache.commons.lang3.StringUtils.isEmpty(tmp)) {
                    return tmp;
                  } else if (tmp.startsWith("array")) {
                    return tmp.replace("array", "").replace("(", "[").replace(")", "]");
                  } else if (tmp.startsWith("map")) {
                    return tmp.replace("map", "")
                        .replace("(", "{")
                        .replace(")", "}")
                        .replace(", ", "=");
                  } else if (tmp.startsWith("struct")) {
                    return tmp.replace("struct", "")
                        .replace("(", "")
                        .replace(")", "")
                        .replace(", ", ",");
                  }
                  return tmp;
                })
            .collect(Collectors.joining(","));

    List<String> queryResult =
        sql(getSelectAllSql(name)).stream()
            .map(
                line ->
                    Arrays.stream(line)
                        .map(
                            item -> {
                              if (item instanceof Object[]) {
                                return Arrays.stream((Object[]) item)
                                    .map(Object::toString)
                                    .collect(Collectors.joining(","));
                              } else {
                                return item.toString();
                              }
                            })
                        .collect(Collectors.joining(",")))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        1, queryResult.size(), "Should just one row, table content: " + queryResult);
    Assertions.assertEquals(checkValues, queryResult.get(0));
  }

  protected void checkTableRowLevelDelete(SparkTableInfo table) {
    String name = table.getTableIdentifier();
    checkTableReadWrite(table);

    String sourceTableName = "source_table";

    String selectClause =
        table.getColumns().stream()
            .map(
                columnInfo ->
                    String.format(
                        "%s as %s",
                        typeNewConstant.get(columnInfo.getType()), columnInfo.getName()))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    List<SparkColumnInfo> columns = table.getColumns();
    Preconditions.checkArgument(columns.size() > 0, "columns should not be empty");
    SparkColumnInfo onColumn = columns.get(0);

    String onClause =
        String.format(
            "%s.%s = %s.%s", name, onColumn.getName(), sourceTableName, onColumn.getName());

    sql(getRowLevelDeleteTableSql(name, sourceTableName, selectClause, onClause));

    List<Object[]> queryResult = sql(getSelectAllSql(name));
    Assertions.assertEquals(0, queryResult.size(), "Should no rows, table content: " + queryResult);
  }

  protected void checkTableRowLevelInsert(SparkTableInfo table) {
    String name = table.getTableIdentifier();
    List<Object[]> queryResult = sql(getSelectAllSql(name));
    Assertions.assertEquals(0, queryResult.size(), "Should no rows, table content: " + queryResult);

    String sourceTableName = "source_table";

    String selectClause =
        table.getColumns().stream()
            .map(
                columnInfo ->
                    String.format(
                        "%s as %s",
                        typeNewConstant.get(columnInfo.getType()), columnInfo.getName()))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    List<SparkColumnInfo> columns = table.getColumns();
    Preconditions.checkArgument(columns.size() > 0, "columns should not be empty");
    SparkColumnInfo onColumn = columns.get(0);

    String onClause =
        String.format(
            "%s.%s = %s.%s", name, onColumn.getName(), sourceTableName, onColumn.getName());

    sql(getRowLevelDeleteTableSql(name, sourceTableName, selectClause, onClause));

    // do something to match the query result:
    // 1. remove "'" from values, such as 'a' is trans to a
    // 2. remove "array" from values, such as array(1, 2, 3) is trans to [1, 2, 3]
    // 3. remove "map" from values, such as map('a', 1, 'b', 2) is trans to {a=1, b=2}
    // 4. remove "struct" from values, such as struct(1, 'a') is trans to 1,a
    String checkValues =
        table.getColumns().stream()
            .map(columnInfo -> typeNewConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .map(
                s -> {
                  String tmp = org.apache.commons.lang3.StringUtils.remove(s, "'");
                  if (org.apache.commons.lang3.StringUtils.isEmpty(tmp)) {
                    return tmp;
                  } else if (tmp.startsWith("array")) {
                    return tmp.replace("array", "").replace("(", "[").replace(")", "]");
                  } else if (tmp.startsWith("map")) {
                    return tmp.replace("map", "")
                        .replace("(", "{")
                        .replace(")", "}")
                        .replace(", ", "=");
                  } else if (tmp.startsWith("struct")) {
                    return tmp.replace("struct", "")
                        .replace("(", "")
                        .replace(")", "")
                        .replace(", ", ",");
                  }
                  return tmp;
                })
            .collect(Collectors.joining(","));

    List<String> queryResultWithInsert =
        sql(getSelectAllSql(name)).stream()
            .map(
                line ->
                    Arrays.stream(line)
                        .map(
                            item -> {
                              if (item instanceof Object[]) {
                                return Arrays.stream((Object[]) item)
                                    .map(Object::toString)
                                    .collect(Collectors.joining(","));
                              } else {
                                return item.toString();
                              }
                            })
                        .collect(Collectors.joining(",")))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        1,
        queryResultWithInsert.size(),
        "Should just one row, table content: " + queryResultWithInsert);
    Assertions.assertEquals(checkValues, queryResultWithInsert.get(0));
  }

  protected String getCreateSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', age INT)",
        tableName);
  }

  protected List<SparkColumnInfo> getSimpleTableColumn() {
    return Arrays.asList(
        SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkColumnInfo.of("age", DataTypes.IntegerType, null));
  }

  protected String getDefaultDatabase() {
    return "default_db";
  }

  // Helper method to create a simple table, and could use corresponding
  // getSimpleTableColumn to check table column.
  protected void createSimpleTable(String identifier) {
    String createTableSql = getCreateSimpleTableString(identifier);
    sql(createTableSql);
  }

  private void checkTableColumns(
      String tableName, List<SparkColumnInfo> columns, SparkTableInfo tableInfo) {
    SparkTableInfoChecker.create()
        .withName(tableName)
        .withColumns(columns)
        .withComment(null)
        .check(tableInfo);
  }

  // partition expression may contain "'", like a='s'/b=1
  private String getPartitionExpression(SparkTableInfo table, String delimiter) {
    return table.getPartitionedColumns().stream()
        .map(column -> column.getName() + "=" + typeConstant.get(column.getType()))
        .collect(Collectors.joining(delimiter));
  }
}
