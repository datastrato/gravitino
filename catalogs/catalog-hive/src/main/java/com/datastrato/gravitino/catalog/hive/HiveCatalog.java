/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.connector.BaseCatalog;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.ProxyPlugin;
import com.datastrato.gravitino.connector.capability.Capability;
import java.util.Map;
import java.util.Optional;

/** Implementation of a Hive catalog in Gravitino. */
public class HiveCatalog extends BaseCatalog<HiveCatalog> {

  private static final HiveCatalogPropertiesMeta CATALOG_PROPERTIES_META =
      new HiveCatalogPropertiesMeta();

  private static final HiveSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new HiveSchemaPropertiesMetadata();

  private static final HiveTablePropertiesMetadata TABLE_PROPERTIES_META =
      new HiveTablePropertiesMetadata();

  /**
   * Returns the short name of the Hive catalog.
   *
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "hive";
  }

  /**
   * Creates a new instance of {@link HiveCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Hive catalog operations.
   * @return A new instance of {@link HiveCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    HiveCatalogOperations ops = new HiveCatalogOperations();
    return ops;
  }

  @Override
  public Capability newCapability() {
    return new HiveCatalogCapability();
  }

  @Override
  protected Optional<ProxyPlugin> newProxyPlugin(Map<String, String> config) {
    boolean impersonationEnabled =
        (boolean)
            new HiveCatalogPropertiesMeta()
                .getOrDefault(config, HiveCatalogPropertiesMeta.IMPERSONATION_ENABLE);
    if (!impersonationEnabled) {
      return Optional.empty();
    }
    return Optional.of(new HiveProxyPlugin());
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_META;
  }
}
