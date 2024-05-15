/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR;
import static com.datastrato.gravitino.trino.connector.GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CATALOG_ALREADY_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_DUPLICATED_CATALOGS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;

import com.datastrato.gravitino.trino.connector.GravitinoConfig;
import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.jdbc.TrinoDriver;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class dynamically register the Catalog managed by Gravitino into Trino using Trino CREATE
 * CATALOG statement. It allows it to be used in Trino like a regular Trino catalog.
 */
public class CatalogRegister {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogRegister.class);

  private static final int MIN_TRINO_SPI_VERSION = 435;
  private String trinoVersion;
  private String trinoServerURI;
  private Connection connection;
  private boolean isCoordinator;
  private String catalogStoreDirectory;

  private void checkTrinoSpiVersion(ConnectorContext context) {
    this.trinoVersion = context.getSpiVersion();

    int version = Integer.parseInt(context.getSpiVersion());
    if (version < MIN_TRINO_SPI_VERSION) {
      String errmsg =
          String.format(
              "Unsupported trino-%s version. min support version is trino-%d",
              trinoVersion, MIN_TRINO_SPI_VERSION);
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION, errmsg);
    }

    isCoordinator = context.getNodeManager().getCurrentNode().isCoordinator();
  }

  boolean isCoordinator() {
    return isCoordinator = true;
  }

  public void init(ConnectorContext context, GravitinoConfig config) throws Exception {
    checkTrinoSpiVersion(context);

    // Register the catalog
    trinoServerURI = config.getTrinoURI();
    TrinoDriver driver = new TrinoDriver();

    Properties properties = new Properties();
    properties.put("user", "admin");
    connection = driver.connect(config.getTrinoURI(), properties);

    catalogStoreDirectory = config.getCatalogStoreDirectory();
    if (!Files.exists(Path.of(catalogStoreDirectory))) {
      throw new TrinoException(
          GRAVITINO_MISSING_CONFIG,
          "Error config for Trino catalog store directory, file not found");
    }
  }

  private String generateCreateCatalogCommand(String name, GravitinoCatalog gravitinoCatalog)
      throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    String command =
        String.format(
            "CREATE CATALOG %s USING gravitino WITH ( \"%s\" = 'true', \"%s\" = '%s')",
            name,
            GRAVITINO_DYNAMIC_CONNECTOR,
            GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG,
            objectMapper.writeValueAsString(gravitinoCatalog));
    return command;
  }

  private String generateDropCatalogCommand(String name) throws Exception {
    String command = String.format("DROP CATALOG %s ", name);
    return command;
  }

  void registerCatalog(String name, GravitinoCatalog catalog) {
    try {
      String catalogFileName = String.format("%s/%s.properties", catalogStoreDirectory, name);
      File catalogFile = new File(catalogFileName);
      if (catalogFile.exists()) {
        String catalogContents = Files.readString(catalogFile.toPath());
        if (!catalogContents.contains("\"__gravitino.dynamic.connector\" = 'true'")) {
          throw new TrinoException(
              GRAVITINO_DUPLICATED_CATALOGS,
              "Catalog already exists, the catalog isn't load by gravitino");
        } else {
          throw new TrinoException(GRAVITINO_CATALOG_ALREADY_EXISTS, "Catalog already exists");
        }
      }

      String createCatalogCommand = generateCreateCatalogCommand(name, catalog);
      String showCatalogCommand = String.format("SHOW CATALOGS like '%s'", name);
      Exception createCatalogException = null;
      int retries = 3;
      while (retries-- > 0) {
        try {
          Statement statement = connection.createStatement();
          statement.execute(createCatalogCommand);

          statement = connection.createStatement();
          statement.execute(showCatalogCommand);
          ResultSet rs = statement.getResultSet();
          if (rs.next() && rs.getString(1).equals(name)) {
            createCatalogException = null;
            break;
          }
        } catch (Exception e) {
          createCatalogException = e;
          Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        }
      }
      if (createCatalogException != null) {
        throw createCatalogException;
      }
    } catch (Exception e) {
      LOG.error("Failed to register catalog", e);
    }
  }

  void unResisterCatalog(String name) {
    try {
      String command = generateDropCatalogCommand(name);
      connection.createStatement().execute(command);
    } catch (Exception e) {
      LOG.error("Failed to register catalog", e);
    }
  }

  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error("Failed to close connection", e);
    }
  }
}
