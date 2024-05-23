/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.trino.spi.TrinoException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.util.Strings;

public class GravitinoConfig {

  public static String GRAVITINO_DYNAMIC_CONNECTOR = "__gravitino.dynamic.connector";
  public static String GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG =
      "__gravitino.dynamic.connector.catalog.config";
  private static final Map<String, ConfigEntry> CONFIG_DEFINITIONS = new HashMap<>();

  private final Map<String, String> config;

  private static final ConfigEntry GRAVITINO_URI =
      new ConfigEntry(
          "gravitino.uri", "The uri of the gravitino web server", "http://localhost:8090", false);

  private static final ConfigEntry GRAVITINO_METALAKE =
      new ConfigEntry("gravitino.metalake", "The metalake name for used", "", true);

  private static final ConfigEntry GRAVITINO_SIMPLIFY_CATALOG_NAMES =
      new ConfigEntry(
          "gravitino.simplify-catalog-names",
          "Omit metalake prefix for catalog names",
          "true",
          false);

  private static final ConfigEntry DISCOVERY_URI =
      new ConfigEntry(
          "trino.jdbc.uri", "The jdbc uri of Trino server", "jdbc:trino://localhost:8080", false);

  private static final ConfigEntry CATALOG_STORE =
      new ConfigEntry(
          "trino.catalog.store", "The directory of stored catalogs Trino", "etc/catalog", false);

  public GravitinoConfig(Map<String, String> requiredConfig) {
    config = requiredConfig;
    for (Map.Entry<String, ConfigEntry> entry : CONFIG_DEFINITIONS.entrySet()) {
      ConfigEntry configDefinition = entry.getValue();
      if (configDefinition.isRequired && !config.containsKey(configDefinition.key)) {
        String message =
            String.format("Missing gravitino config, %s is required", configDefinition.key);
        throw new TrinoException(GRAVITINO_MISSING_CONFIG, message);
      }
    }
  }

  public String getURI() {
    return config.getOrDefault(GRAVITINO_URI.key, GRAVITINO_URI.defaultValue);
  }

  public String getMetalake() {
    return config.getOrDefault(GRAVITINO_METALAKE.key, GRAVITINO_METALAKE.defaultValue);
  }

  public boolean simplifyCatalogNames() {
    return Boolean.parseBoolean(
        config.getOrDefault(
            GRAVITINO_SIMPLIFY_CATALOG_NAMES.key, GRAVITINO_SIMPLIFY_CATALOG_NAMES.defaultValue));
  }

  boolean isDynamicConnector() {
    // 'isDynamicConnector' indicates whether the connector is user-configured within Trino or
    // loaded from the Gravitino server.
    // When a connector is loaded via Trino configuration,
    // it is static and will always create an instance of GravitinoSystemConnector.
    // Otherwise, it is dynamically loaded from the Gravitino server,
    // in which case the connector's configuration is set to '__gravitino.dynamic.connector=true'.
    // It is dynamic and will create an instance of GravitinoConnector.
    return config.getOrDefault(GRAVITINO_DYNAMIC_CONNECTOR, "false").equals("true");
  }

  public String getCatalogConfig() throws JsonProcessingException {
    return config.get(GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG);
  }

  public String getTrinoURI() {
    return config.getOrDefault(DISCOVERY_URI.key, DISCOVERY_URI.defaultValue);
  }

  public String getCatalogStoreDirectory() {
    return config.getOrDefault(CATALOG_STORE.key, CATALOG_STORE.defaultValue);
  }

  public String toCatalogConfig() {
    List<String> stringList = new ArrayList<>();
    for (Map.Entry<String, ConfigEntry> entry : CONFIG_DEFINITIONS.entrySet()) {
      String value = config.get(entry.getKey());
      if (value != null) {
        stringList.add(String.format("\"%s\"='%s'", entry.getKey(), value));
      }
    }
    return Strings.join(stringList, ',');
  }

  static class ConfigEntry {
    final String key;
    final String description;
    final String defaultValue;
    final boolean isRequired;

    ConfigEntry(String key, String description, String defaultValue, boolean isRequired) {
      this.key = key;
      this.description = description;
      this.defaultValue = defaultValue;
      this.isRequired = isRequired;

      CONFIG_DEFINITIONS.put(key, this);
    }
  }
}
