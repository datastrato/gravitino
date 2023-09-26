/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorContext;
import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorFactory;
import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.graviton.trino.connector.catalog.CatalogInjector;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

public class GravitonConnectorFactory implements ConnectorFactory {

  private static final String DEFAULT_CONNECTOR_NAME = "graviton";

  private CatalogConnectorManager catalogConnectorManager;

  @Override
  public String getName() {
    return DEFAULT_CONNECTOR_NAME;
  }

  /**
   * This function call by trino create connector. It'create DummyGravitonConnector at first. other
   * time's it get GravitonConnector by CatalogManger
   *
   * @param catalogName
   * @param requiredConfig
   * @param context
   * @return
   */
  @Override
  public Connector create(
      String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
    Preconditions.checkArgument(requiredConfig != null, "requiredConfig is not null");

    synchronized (this) {
      if (catalogConnectorManager == null) {
        try {
          CatalogInjector catalogInjector = new CatalogInjector();
          catalogInjector.bindCatalogManager(context);
          CatalogConnectorFactory catalogConnectorFactory =
              new CatalogConnectorFactory(catalogInjector);

          GravitonConfig config = new GravitonConfig(requiredConfig);
          catalogConnectorManager =
              new CatalogConnectorManager(catalogInjector, catalogConnectorFactory);
          catalogConnectorManager.config(config);
          catalogConnectorManager.start();

        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        // Default GravitonConnector named "graviton" is just using to load CatalogConnectorManager,
        // that's dynamically loading catalogs from graviton server.
        return new DummyGravitonConnector();
      }
    }

    Preconditions.checkNotNull(catalogConnectorManager, "catalogConnectorManager is not null");

    CatalogConnectorContext catalogConnectorContext =
        catalogConnectorManager.getCatalogConnector(catalogName);
    Preconditions.checkNotNull(catalogConnectorContext, "catalogConnector is not null");

    return catalogConnectorContext.getConnector();
  }
}
