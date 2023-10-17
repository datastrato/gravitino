/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.CATALOG_BACKEND_NAME;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_INITIALIZE;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_PASSWORD;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_USER;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.URI;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.WAREHOUSE;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;

public class IcebergConfig extends Config {

  public static final ConfigEntry<String> CATALOG_BACKEND =
      new ConfigBuilder(CATALOG_BACKEND_NAME)
          .doc("Choose the implementation of the Iceberg catalog")
          .version(DEFAULT_VERSION)
          .stringConf()
          .createWithDefault("memory");

  public static final ConfigEntry<String> CATALOG_WAREHOUSE =
      new ConfigBuilder(WAREHOUSE)
          .doc("The warehouse config of the Iceberg catalog")
          .version(DEFAULT_VERSION)
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> CATALOG_URI =
      new ConfigBuilder(URI)
          .doc("The uri config of the Iceberg catalog")
          .version(DEFAULT_VERSION)
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> JDBC_USER =
      new ConfigBuilder(ICEBERG_JDBC_USER)
          .doc("The username of the Jdbc")
          .version(DEFAULT_VERSION)
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> JDBC_PASSWORD =
      new ConfigBuilder(ICEBERG_JDBC_PASSWORD)
          .doc("The password of the Jdbc")
          .version(DEFAULT_VERSION)
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<Boolean> JDBC_INIT_TABLES =
      new ConfigBuilder(ICEBERG_JDBC_INITIALIZE)
          .doc("whether to initialize meta tables when create Jdbc catalog")
          .version(DEFAULT_VERSION)
          .booleanConf()
          .createWithDefault(Boolean.TRUE);

  public IcebergConfig() {
    super(false);
  }
}
