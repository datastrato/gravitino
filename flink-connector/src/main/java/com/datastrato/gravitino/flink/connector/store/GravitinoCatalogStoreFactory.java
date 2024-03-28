/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.store;

import static com.datastrato.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.GRAVITINO;
import static com.datastrato.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.METALAKE_NAME;
import static com.datastrato.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions.METALAKE_URL;
import static org.apache.flink.table.factories.FactoryUtil.createCatalogStoreFactoryHelper;

import com.datastrato.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class GravitinoCatalogStoreFactory implements CatalogStoreFactory {
  private GravitinoCatalogManager catalogManager;

  @Override
  public CatalogStore createCatalogStore() {
    return new GravitinoCatalogStore(catalogManager);
  }

  @Override
  public void open(Context context) throws CatalogException {
    FactoryUtil.FactoryHelper<CatalogStoreFactory> factoryHelper =
        createCatalogStoreFactoryHelper(this, context);
    factoryHelper.validate();

    ReadableConfig options = factoryHelper.getOptions();
    String metalakeUrl = options.get(METALAKE_URL);
    String metalakeName = options.get(METALAKE_NAME);
    this.catalogManager = GravitinoCatalogManager.create(metalakeUrl, metalakeName);
  }

  @Override
  public void close() throws CatalogException {
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  @Override
  public String factoryIdentifier() {
    return GRAVITINO;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return null;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return null;
  }
}
