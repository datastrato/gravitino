/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.SupportsSchemas;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.datastrato.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

public abstract class CatalogPaimonBaseIT extends AbstractIT {

  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected String WAREHOUSE;
  protected String URIS;
  protected String TYPE;

  private static final String provider = "lakehouse-paimon";
  private static final String catalog_comment = "catalog_comment";
  private static final String schema_comment = "schema_comment";
  private static final String table_comment = "table_comment";
  private static final String PAIMON_COL_NAME1 = "paimon_col_name1";
  private static final String PAIMON_COL_NAME2 = "paimon_col_name2";
  private static final String PAIMON_COL_NAME3 = "paimon_col_name3";
  private static final String PAIMON_COL_NAME4 = "paimon_col_name4";
  private String metalakeName = GravitinoITUtils.genRandomName("paimon_it_metalake");
  private String catalogName = GravitinoITUtils.genRandomName("paimon_it_catalog");
  private String schemaName = GravitinoITUtils.genRandomName("paimon_it_schema");
  private String tableName = GravitinoITUtils.genRandomName("paimon_it_table");
  private static String INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE = "INSERT INTO paimon.%s VALUES %s";
  private static final String SELECT_ALL_TEMPLATE = "SELECT * FROM paimon.%s";
  private GravitinoMetalake metalake;
  private Catalog catalog;
  private org.apache.paimon.catalog.Catalog paimonCatalog;
  private SparkSession spark;
  private Map<String, String> catalogProperties;

  @BeforeAll
  public void startup() {
    containerSuite.startHiveContainer();
    catalogProperties = initPaimonCatalogProperties();
    createMetalake();
    createCatalog();
    createSchema();
    initSparkEnv();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    metalake.dropCatalog(catalogName);
    client.dropMetalake(metalakeName);
  }

  @AfterEach
  private void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  protected abstract Map<String, String> initPaimonCatalogProperties();

  @Test
  void testPaimonSchemaOperations() throws DatabaseNotExistException {
    SupportsSchemas schemas = catalog.asSchemas();

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Map<String, String> schemaProperties = Maps.newHashMap();
    schemaProperties.put("key1", "val1");
    schemaProperties.put("key2", "val2");
    schemas.createSchema(schemaIdent.name(), schema_comment, schemaProperties);

    Set<String> schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertTrue(schemaNames.contains(testSchemaName));
    List<String> paimonDatabaseNames = paimonCatalog.listDatabases();
    Assertions.assertTrue(paimonDatabaseNames.contains(testSchemaName));

    // load schema check.
    Schema schema = schemas.loadSchema(schemaIdent.name());
    // database properties is empty for Paimon FilesystemCatalog.
    Assertions.assertTrue(schema.properties().isEmpty());
    Assertions.assertTrue(paimonCatalog.loadDatabaseProperties(schemaIdent.name()).isEmpty());

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent.name(), schema_comment, emptyMap));

    // alter schema check.
    // alter schema operation is unsupported.
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> schemas.alterSchema(schemaIdent.name(), SchemaChange.setProperty("k1", "v1")));

    // drop schema check.
    schemas.dropSchema(schemaIdent.name(), false);
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent.name()));
    Assertions.assertThrows(
        DatabaseNotExistException.class,
        () -> {
          paimonCatalog.loadDatabaseProperties(schemaIdent.name());
        });

    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    Assertions.assertFalse(schemas.dropSchema(schemaIdent.name(), false));
    Assertions.assertFalse(schemas.dropSchema("no-exits", false));

    // list schema check.
    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    paimonDatabaseNames = paimonCatalog.listDatabases();
    Assertions.assertFalse(paimonDatabaseNames.contains(testSchemaName));
  }

  @Test
  void testCreateTableWithNullComment() {
    Column[] columns = createColumns();
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(tableIdentifier, columns, null, null, null, null, null);
    Assertions.assertNull(createdTable.comment());

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertNull(loadTable.comment());
  }

  @Test
  void testCreateAndLoadPaimonTable()
      throws org.apache.paimon.catalog.Catalog.TableNotExistException {
    // Create table from Gravitino API
    Column[] columns = createColumns();

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);
    Distribution distribution = Distributions.NONE;

    Transform[] partitioning = Transforms.EMPTY_TRANSFORM;
    SortOrder[] sortOrders = SortOrders.NONE;
    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(
            tableIdentifier,
            columns,
            table_comment,
            properties,
            partitioning,
            distribution,
            sortOrders);
    Assertions.assertEquals(createdTable.name(), tableName);
    Map<String, String> resultProp = createdTable.properties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    Assertions.assertEquals(createdTable.columns().length, columns.length);

    for (int i = 0; i < columns.length; i++) {
      Assertions.assertEquals(DTOConverters.toDTO(columns[i]), createdTable.columns()[i]);
    }

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(tableName, loadTable.name());
    Assertions.assertEquals(table_comment, loadTable.comment());
    resultProp = loadTable.properties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }
    Assertions.assertEquals(loadTable.columns().length, columns.length);
    for (int i = 0; i < columns.length; i++) {
      Assertions.assertEquals(DTOConverters.toDTO(columns[i]), loadTable.columns()[i]);
    }

    // catalog load check
    org.apache.paimon.table.Table table =
        paimonCatalog.getTable(Identifier.create(schemaName, tableName));
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertTrue(table.comment().isPresent());
    Assertions.assertEquals(table_comment, table.comment().get());
    resultProp = table.options();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertTrue(resultProp.containsKey(entry.getKey()));
      Assertions.assertEquals(entry.getValue(), resultProp.get(entry.getKey()));
    }

    Assertions.assertInstanceOf(FileStoreTable.class, table);
    FileStoreTable fileStoreTable = (FileStoreTable) table;

    TableSchema schema = fileStoreTable.schema();
    Assertions.assertEquals(schema.fields().size(), columns.length);
    for (int i = 0; i < columns.length; i++) {
      Assertions.assertEquals(columns[i].name(), schema.fieldNames().get(i));
    }
    Assertions.assertEquals(partitioning.length, fileStoreTable.partitionKeys().size());

    Assertions.assertThrows(
        TableAlreadyExistsException.class,
        () ->
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdentifier,
                    columns,
                    table_comment,
                    properties,
                    Transforms.EMPTY_TRANSFORM,
                    distribution,
                    sortOrders));
  }

  @Test
  void testTimestampTypeConversion()
      throws org.apache.paimon.catalog.Catalog.TableNotExistException {
    Column col1 = Column.of("paimon_column_1", Types.TimestampType.withTimeZone(), "col_1_comment");
    Column col2 =
        Column.of("paimon_column_2", Types.TimestampType.withoutTimeZone(), "col_2_comment");

    Column[] columns = new Column[] {col1, col2};

    String timestampTableName = "timestamp_table";

    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, timestampTableName);

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table createdTable =
        tableCatalog.createTable(tableIdentifier, columns, table_comment, properties);
    Assertions.assertEquals("paimon_column_1", createdTable.columns()[0].name());
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(), createdTable.columns()[0].dataType());
    Assertions.assertEquals("col_1_comment", createdTable.columns()[0].comment());
    Assertions.assertTrue(createdTable.columns()[0].nullable());

    Assertions.assertEquals("paimon_column_2", createdTable.columns()[1].name());
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(), createdTable.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", createdTable.columns()[1].comment());
    Assertions.assertTrue(createdTable.columns()[1].nullable());

    Table loadTable = tableCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals("paimon_column_1", loadTable.columns()[0].name());
    Assertions.assertEquals(Types.TimestampType.withTimeZone(), loadTable.columns()[0].dataType());
    Assertions.assertEquals("col_1_comment", loadTable.columns()[0].comment());
    Assertions.assertTrue(loadTable.columns()[0].nullable());

    Assertions.assertEquals("paimon_column_2", loadTable.columns()[1].name());
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(), loadTable.columns()[1].dataType());
    Assertions.assertEquals("col_2_comment", loadTable.columns()[1].comment());
    Assertions.assertTrue(loadTable.columns()[1].nullable());

    org.apache.paimon.table.Table table =
        paimonCatalog.getTable(Identifier.create(schemaName, timestampTableName));
    Assertions.assertInstanceOf(FileStoreTable.class, table);
    FileStoreTable fileStoreTable = (FileStoreTable) table;
    TableSchema tableSchema = fileStoreTable.schema();
    Assertions.assertEquals("paimon_column_1", tableSchema.fields().get(0).name());
    Assertions.assertEquals(
        new LocalZonedTimestampType().nullable(), tableSchema.fields().get(0).type());
    Assertions.assertEquals("col_1_comment", tableSchema.fields().get(0).description());

    Assertions.assertEquals("paimon_column_2", tableSchema.fields().get(1).name());
    Assertions.assertEquals(new TimestampType().nullable(), tableSchema.fields().get(1).type());
    Assertions.assertEquals("col_2_comment", tableSchema.fields().get(1).description());
  }

  @Test
  void testListAndDropPaimonTable() throws DatabaseNotExistException {
    Column[] columns = createColumns();

    String tableName1 = "table_1";

    NameIdentifier table1 = NameIdentifier.of(schemaName, tableName1);

    Map<String, String> properties = createProperties();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        table1,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);
    NameIdentifier[] nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("table_1", nameIdentifiers[0].name());

    List<String> tableIdentifiers = paimonCatalog.listTables(schemaName);
    Assertions.assertEquals(1, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0));

    String tableName2 = "table_2";

    NameIdentifier table2 = NameIdentifier.of(schemaName, tableName2);
    tableCatalog.createTable(
        table2,
        columns,
        table_comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);
    nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(2, nameIdentifiers.length);
    Assertions.assertEquals("table_1", nameIdentifiers[0].name());
    Assertions.assertEquals("table_2", nameIdentifiers[1].name());

    tableIdentifiers = paimonCatalog.listTables(schemaName);
    Assertions.assertEquals(2, tableIdentifiers.size());
    Assertions.assertEquals("table_1", tableIdentifiers.get(0));
    Assertions.assertEquals("table_2", tableIdentifiers.get(1));

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(table1));

    nameIdentifiers = tableCatalog.listTables(Namespace.of(schemaName));
    Assertions.assertEquals(1, nameIdentifiers.length);
    Assertions.assertEquals("table_2", nameIdentifiers[0].name());

    Assertions.assertDoesNotThrow(() -> tableCatalog.dropTable(table2));
    Namespace schemaNamespace = Namespace.of(schemaName);
    nameIdentifiers = tableCatalog.listTables(schemaNamespace);
    Assertions.assertEquals(0, nameIdentifiers.length);

    Assertions.assertEquals(0, paimonCatalog.listTables(schemaName).size());
  }

  @Test
  public void testAlterPaimonTable() {
    // TODO: support alter table
  }

  @Test
  void testOperationDataOfPaimonTable() {
    Column[] columns = createColumns();
    String testTableName = GravitinoITUtils.genRandomName("test_table");
    SortOrder[] sortOrders = SortOrders.NONE;
    Transform[] transforms = Transforms.EMPTY_TRANSFORM;
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, testTableName),
            columns,
            table_comment,
            createProperties(),
            transforms,
            Distributions.NONE,
            sortOrders);
    List<String> values = getValues();
    String dbTable = String.join(".", schemaName, testTableName);
    // insert data
    String insertSQL =
        String.format(INSERT_BATCH_WITHOUT_PARTITION_TEMPLATE, dbTable, String.join(", ", values));
    spark.sql(insertSQL);

    // select data
    Dataset<Row> sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, dbTable));
    Assertions.assertEquals(4, sql.count());
    Row[] result = (Row[]) sql.sort(PAIMON_COL_NAME1).collect();
    LocalDate currentDate = LocalDate.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    for (int i = 0; i < result.length; i++) {
      LocalDate previousDay = currentDate.minusDays(i + 1);
      Assertions.assertEquals(
          String.format(
              "[%s,%s,data%s,[%s,string%s,[%s,inner%s]]]",
              i + 1, previousDay.format(formatter), i + 1, (i + 1) * 10, i + 1, i + 1, i + 1),
          result[i].toString());
    }

    // update data
    spark.sql(
        String.format(
            "UPDATE paimon.%s SET %s = 100 WHERE %s = 1",
            dbTable, PAIMON_COL_NAME1, PAIMON_COL_NAME1));
    sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, dbTable));
    Assertions.assertEquals(4, sql.count());
    result = (Row[]) sql.sort(PAIMON_COL_NAME1).collect();
    for (int i = 0; i < result.length; i++) {
      if (i == result.length - 1) {
        LocalDate previousDay = currentDate.minusDays(1);
        Assertions.assertEquals(
            String.format(
                "[100,%s,data%s,[%s,string%s,[%s,inner%s]]]",
                previousDay.format(formatter), 1, 10, 1, 1, 1),
            result[i].toString());
      } else {
        LocalDate previousDay = currentDate.minusDays(i + 2);
        Assertions.assertEquals(
            String.format(
                "[%s,%s,data%s,[%s,string%s,[%s,inner%s]]]",
                i + 2, previousDay.format(formatter), i + 2, (i + 2) * 10, i + 2, i + 2, i + 2),
            result[i].toString());
      }
    }
    // delete data
    spark.sql(String.format("DELETE FROM paimon.%s WHERE %s = 100", dbTable, PAIMON_COL_NAME1));
    sql = spark.sql(String.format(SELECT_ALL_TEMPLATE, dbTable));
    Assertions.assertEquals(3, sql.count());
    result = (Row[]) sql.sort(PAIMON_COL_NAME1).collect();
    for (int i = 0; i < result.length; i++) {
      LocalDate previousDay = currentDate.minusDays(i + 2);
      Assertions.assertEquals(
          String.format(
              "[%s,%s,data%s,[%s,string%s,[%s,inner%s]]]",
              i + 2, previousDay.format(formatter), i + 2, (i + 2) * 10, i + 2, i + 2, i + 2),
          result[i].toString());
    }
  }

  private static @NotNull List<String> getValues() {
    List<String> values = new ArrayList<>();
    for (int i = 1; i < 5; i++) {
      String structValue =
          String.format(
              "STRUCT(%d, 'string%d', %s)",
              i * 10, // integer_field
              i, // string_field
              String.format(
                  "STRUCT(%d, 'inner%d')",
                  i, i) // struct_field, alternating NULL and non-NULL values
              );
      values.add(
          String.format("(%d, date_sub(current_date(), %d), 'data%d', %s)", i, i, i, structValue));
    }
    return values;
  }

  private void clearTableAndSchema() {
    if (catalog.asSchemas().schemaExists(schemaName)) {
      catalog.asSchemas().dropSchema(schemaName, true);
    }
  }

  private void createMetalake() {
    GravitinoMetalake createdMetalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private void createCatalog() {
    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, catalog_comment, catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);
    catalog = loadCatalog;

    String type =
        catalogProperties
            .get(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND)
            .toLowerCase(Locale.ROOT);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(type), "Paimon Catalog backend type can not be null or empty.");
    catalogProperties.put(PaimonCatalogPropertiesMetadata.PAIMON_METASTORE, type);
    paimonCatalog = CatalogUtils.loadCatalogBackend(new PaimonConfig(catalogProperties));
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();
    prop.put("key1", "val1");
    prop.put("key2", "val2");

    Schema createdSchema = catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    // database properties is empty for Paimon FilesystemCatalog.
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    Assertions.assertTrue(loadSchema.properties().isEmpty());
  }

  private Column[] createColumns() {
    Column col1 = Column.of(PAIMON_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(PAIMON_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(PAIMON_COL_NAME3, Types.StringType.get(), "col_3_comment");
    Types.StructType structTypeInside =
        Types.StructType.of(
            Types.StructType.Field.notNullField("integer_field_inside", Types.IntegerType.get()),
            Types.StructType.Field.notNullField(
                "string_field_inside", Types.StringType.get(), "string field inside"));
    Types.StructType structType =
        Types.StructType.of(
            Types.StructType.Field.notNullField("integer_field", Types.IntegerType.get()),
            Types.StructType.Field.notNullField(
                "string_field", Types.StringType.get(), "string field"),
            Types.StructType.Field.nullableField("struct_field", structTypeInside, "struct field"));
    Column col4 = Column.of(PAIMON_COL_NAME4, structType, "col_4_comment");
    return new Column[] {col1, col2, col3, col4};
  }

  private Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    return properties;
  }

  private void initSparkEnv() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Paimon Catalog integration test")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", WAREHOUSE)
            .config(
                "spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .enableHiveSupport()
            .getOrCreate();
  }
}
