/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.integration.test;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig.AUTH_TYPE_KEY;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosConfig.KEY_TAB_URI_KEY;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosConfig.PRINCIPAL_KEY;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.client.KerberosTokenProvider;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonKerberosFilesystemIT extends AbstractIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(CatalogPaimonKerberosFilesystemIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String SDK_KERBEROS_PRINCIPAL_KEY = "client.kerberos.principal";
  private static final String SDK_KERBEROS_KEYTAB_KEY = "client.kerberos.keytab";

  private static final String GRAVITINO_CLIENT_PRINCIPAL = "gravitino_client@HADOOPKRB";
  private static final String GRAVITINO_CLIENT_KEYTAB = "/gravitino_client.keytab";

  private static final String GRAVITINO_SERVER_PRINCIPAL = "HTTP/localhost@HADOOPKRB";
  private static final String GRAVITINO_SERVER_KEYTAB = "/gravitino_server.keytab";

  private static final String HDFS_CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static final String HDFS_CLIENT_KEYTAB = "/client.keytab";

  private static String TMP_DIR;

  private static HiveContainer kerberosHiveContainer;

  private static GravitinoAdminClient adminClient;

  private static final String METALAKE_NAME = GravitinoITUtils.genRandomName("test_metalake");
  private static final String CATALOG_NAME = GravitinoITUtils.genRandomName("test_catalog");
  private static final String SCHEMA_NAME = GravitinoITUtils.genRandomName("test_schema");
  private static final String TABLE_NAME = GravitinoITUtils.genRandomName("test_table");

  private static String TYPE;
  private static String WAREHOUSE;

  private static final String FILESYSTEM_COL_NAME1 = "col1";
  private static final String FILESYSTEM_COL_NAME2 = "col2";
  private static final String FILESYSTEM_COL_NAME3 = "col3";

  @BeforeAll
  public static void startIntegrationTest() {
    containerSuite.startKerberosHiveContainer();
    kerberosHiveContainer = containerSuite.getKerberosHiveContainer();

    TYPE = "filesystem";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/paimon_catalog_warehouse/",
            kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);

    try {
      File baseDir = new File(System.getProperty("java.io.tmpdir"));
      File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
      file.deleteOnExit();
      TMP_DIR = file.getAbsolutePath();

      // Prepare kerberos related-config;
      prepareKerberosConfig();

      // Config kerberos configuration for Gravitino server
      addKerberosConfig();

      // Start Gravitino server
      AbstractIT.startIntegrationTest();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public static void stop() {
    // Reset the UGI
    UserGroupInformation.reset();

    LOG.info("krb5 path: {}", System.getProperty("java.security.krb5.conf"));
    // Clean up the kerberos configuration
    System.clearProperty("java.security.krb5.conf");
    System.clearProperty("sun.security.krb5.debug");

    AbstractIT.client = null;
  }

  private static void prepareKerberosConfig() throws Exception {
    // Keytab of the Gravitino SDK client
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/gravitino_client.keytab", TMP_DIR + GRAVITINO_CLIENT_KEYTAB);

    // Keytab of the Gravitino server
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/gravitino_server.keytab", TMP_DIR + GRAVITINO_SERVER_KEYTAB);

    // Keytab of Gravitino server to connector to Hive
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/etc/admin.keytab", TMP_DIR + HDFS_CLIENT_KEYTAB);

    String tmpKrb5Path = TMP_DIR + "/krb5.conf_tmp";
    String krb5Path = TMP_DIR + "/krb5.conf";
    kerberosHiveContainer.getContainer().copyFileFromContainer("/etc/krb5.conf", tmpKrb5Path);

    // Modify the krb5.conf and change the kdc and admin_server to the container IP
    String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();
    String content = FileUtils.readFileToString(new File(tmpKrb5Path), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File(krb5Path), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}, path: {}", content, krb5Path);
    System.setProperty("java.security.krb5.conf", krb5Path);
    System.setProperty("sun.security.krb5.debug", "true");

    refreshKerberosConfig();
    KerberosName.resetDefaultRealm();

    LOG.info("Kerberos default realm: {}", KerberosUtil.getDefaultRealm());
  }

  private static void refreshKerberosConfig() {
    Class<?> classRef;
    try {
      if (System.getProperty("java.vendor").contains("IBM")) {
        classRef = Class.forName("com.ibm.security.krb5.internal.Config");
      } else {
        classRef = Class.forName("sun.security.krb5.Config");
      }

      Method refershMethod = classRef.getMethod("refresh");
      refershMethod.invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void addKerberosConfig() {
    AbstractIT.customConfigs.put("gravitino.authenticator", "kerberos");
    AbstractIT.customConfigs.put(
        "gravitino.authenticator.kerberos.principal", GRAVITINO_SERVER_PRINCIPAL);
    AbstractIT.customConfigs.put(
        "gravitino.authenticator.kerberos.keytab", TMP_DIR + GRAVITINO_SERVER_KEYTAB);
    AbstractIT.customConfigs.put(SDK_KERBEROS_KEYTAB_KEY, TMP_DIR + GRAVITINO_CLIENT_KEYTAB);
    AbstractIT.customConfigs.put(SDK_KERBEROS_PRINCIPAL_KEY, GRAVITINO_CLIENT_PRINCIPAL);
  }

  @Test
  void testPaimonWithKerberosAndUserImpersonation() {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(METALAKE_NAME, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthenticationConfig.IMPERSONATION_ENABLE_KEY, "true");
    properties.put(AUTH_TYPE_KEY, "kerberos");
    properties.put(KEY_TAB_URI_KEY, TMP_DIR + HDFS_CLIENT_KEYTAB);
    properties.put(PRINCIPAL_KEY, HDFS_CLIENT_PRINCIPAL);
    properties.put(CATALOG_BYPASS_PREFIX + "fs.defaultFS", defaultBaseLocation());
    properties.put(CATALOG_BYPASS_PREFIX + "fs.hdfs.impl.disable.cache", "true");

    properties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    properties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);

    Catalog catalog =
        gravitinoMetalake.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-paimon", "comment", properties);

    // The FilesystemCatalog would create the warehouse directory if it doesn't exist
    // So we need to create the warehouse directory first to avoid the permission denied error for
    // user 'cli'
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-mkdir", "/user/hive/paimon_catalog_warehouse");

    //    // Test create schema
    //    Exception exception =
    //        Assertions.assertThrows(
    //            Exception.class,
    //            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment",
    // ImmutableMap.of()));
    //    String exceptionMessage = Throwables.getStackTraceAsString(exception);
    //
    //    // Make sure the real user is 'cli'
    //    Assertions.assertTrue(exceptionMessage.contains("Permission denied: user=cli,
    // access=WRITE"));

    // Now try to permit the user to create the schema again
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chmod", "-R", "777", "/user/hive/paimon_catalog_warehouse");
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));

    // Create table
    NameIdentifier tableNameIdentifier = NameIdentifier.of(SCHEMA_NAME, TABLE_NAME);
    catalog
        .asTableCatalog()
        .createTable(
            tableNameIdentifier,
            createColumns(),
            "",
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            SortOrders.NONE);

    // Load table
    Table loadTable = catalog.asTableCatalog().loadTable(tableNameIdentifier);
    Assertions.assertEquals(loadTable.name(), tableNameIdentifier.name());

    // TODO: Alter table is unsupported now.

    // Drop table
    catalog.asTableCatalog().dropTable(tableNameIdentifier);
    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> catalog.asTableCatalog().tableExists(tableNameIdentifier));

    // Drop schema
    catalog.asSchemas().dropSchema(SCHEMA_NAME, false);
    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> catalog.asSchemas().schemaExists(SCHEMA_NAME));

    // Drop catalog
    Assertions.assertTrue(gravitinoMetalake.dropCatalog(CATALOG_NAME));
  }

  @Test
  void testPaimonWithKerberos() {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    String metalakeName = GravitinoITUtils.genRandomName("test_metalake");
    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(metalakeName, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AUTH_TYPE_KEY, "kerberos");
    // Not user impersonation here
    properties.put(KEY_TAB_URI_KEY, TMP_DIR + HDFS_CLIENT_KEYTAB);
    properties.put(PRINCIPAL_KEY, HDFS_CLIENT_PRINCIPAL);
    properties.put(CATALOG_BYPASS_PREFIX + "fs.defaultFS", defaultBaseLocation());
    properties.put(CATALOG_BYPASS_PREFIX + "fs.hdfs.impl.disable.cache", "true");

    properties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    properties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);

    Catalog catalog =
        gravitinoMetalake.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-paimon", "comment", properties);

    // Test create schema
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));
    String exceptionMessage = Throwables.getStackTraceAsString(exception);

    // Make sure the real user is 'cli' because no impersonation here.
    Assertions.assertTrue(exceptionMessage.contains("Permission denied: user=cli, access=WRITE"));

    // Now try to permit the user to create the schema again
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-mkdir", "/user/hive/paimon_catalog_warehouse");
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chmod", "-R", "777", "/user/hive/paimon_catalog_warehouse");
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));

    // Create table
    NameIdentifier tableNameIdentifier = NameIdentifier.of(SCHEMA_NAME, TABLE_NAME);
    catalog
        .asTableCatalog()
        .createTable(
            tableNameIdentifier,
            createColumns(),
            "",
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            SortOrders.NONE);

    // Load table
    Table loadTable = catalog.asTableCatalog().loadTable(tableNameIdentifier);
    Assertions.assertEquals(loadTable.name(), tableNameIdentifier.name());

    // TODO: Alter table is unsupported now.

    // Drop table
    catalog.asTableCatalog().dropTable(tableNameIdentifier);
    Assertions.assertFalse(catalog.asTableCatalog().tableExists(tableNameIdentifier));

    // Drop schema
    catalog.asSchemas().dropSchema(SCHEMA_NAME, false);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(SCHEMA_NAME));

    // Drop catalog
    Assertions.assertTrue(gravitinoMetalake.dropCatalog(CATALOG_NAME));
  }

  private static Column[] createColumns() {
    Column col1 = Column.of(FILESYSTEM_COL_NAME1, Types.IntegerType.get(), "col_1_comment");
    Column col2 = Column.of(FILESYSTEM_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(FILESYSTEM_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  private static String defaultBaseLocation() {
    return String.format(
        "hdfs://%s:%d",
        containerSuite.getKerberosHiveContainer().getContainerIpAddress(),
        HiveContainer.HDFS_DEFAULTFS_PORT);
  }
}
