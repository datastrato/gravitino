/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.authentication.AuthenticationConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.authentication.kerberos.KerberosConfig;
import com.datastrato.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
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

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  // Map that maintains the mapping of keys in Gravitino to that in Iceberg, for example, users
  // will only need to set the configuration 'catalog-backend' in Gravitino and Gravitino will
  // change it to `catalogType` automatically and pass it to Iceberg.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_ICEBERG =
      ImmutableMap.of(
          CATALOG_BACKEND,
          CATALOG_BACKEND,
          GRAVITINO_JDBC_DRIVER,
          GRAVITINO_JDBC_DRIVER,
          GRAVITINO_JDBC_USER,
          ICEBERG_JDBC_USER,
          GRAVITINO_JDBC_PASSWORD,
          ICEBERG_JDBC_PASSWORD,
          URI,
          URI,
          WAREHOUSE,
          WAREHOUSE,
          CATALOG_BACKEND_NAME,
          CATALOG_BACKEND_NAME);

  public static final Map<String, String> KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND =
      ImmutableMap.of(
          KerberosConfig.PRINCIPAL_KEY,
          KerberosConfig.PRINCIPAL_KEY,
          KerberosConfig.KET_TAB_URI_KEY,
          KerberosConfig.KET_TAB_URI_KEY,
          KerberosConfig.CHECK_INTERVAL_SEC_KEY,
          KerberosConfig.CHECK_INTERVAL_SEC_KEY,
          KerberosConfig.FETCH_TIMEOUT_SEC_KEY,
          KerberosConfig.FETCH_TIMEOUT_SEC_KEY,
          AuthenticationConfig.IMPERSONATION_ENABLE_KEY,
          AuthenticationConfig.IMPERSONATION_ENABLE_KEY,
          AuthenticationConfig.AUTH_TYPE_KEY,
          AuthenticationConfig.AUTH_TYPE_KEY);

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            enumImmutablePropertyEntry(
                CATALOG_BACKEND,
                "Iceberg catalog type choose properties",
                true,
                IcebergCatalogBackend.class,
                null,
                false,
                false),
            stringRequiredPropertyEntry(URI, "Iceberg catalog uri config", false, false),
            stringRequiredPropertyEntry(
                WAREHOUSE, "Iceberg catalog warehouse config", false, false));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap(BASIC_CATALOG_PROPERTY_ENTRIES);
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    result.putAll(KerberosConfig.KERBEROS_PROPERTY_ENTRIES);
    result.putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES);
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  public Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> gravitinoConfig = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_ICEBERG.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_ICEBERG.get(key), value);
          }

          if (KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND.containsKey(key)) {
            gravitinoConfig.put(KERBEROS_CONFIGURATION_FOR_HIVE_BACKEND.get(key), value);
          }
        });
    return gravitinoConfig;
  }
}
