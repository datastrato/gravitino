/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.store;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.AbstractCatalogStore;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.exceptions.CatalogException;

public class GravitinoCatalogStore extends AbstractCatalogStore {

  private GravitinoCatalogManager gravitinoCatalogManager;

  public GravitinoCatalogStore(GravitinoCatalogManager catalogManager) {
    this.gravitinoCatalogManager = catalogManager;
  }

  @Override
  public void storeCatalog(String catalogName, CatalogDescriptor descriptor)
      throws CatalogException {
    String provider =
        descriptor.getConfiguration().getString(GravitinoCatalogStoreFactoryOptions.PROVIDER);
    // TODO: add provider type
    gravitinoCatalogManager.createCatalog(
        catalogName, null, "", provider, descriptor.getConfiguration().toMap());
  }

  @Override
  public void removeCatalog(String name, boolean b) throws CatalogException {}

  @Override
  public Optional<CatalogDescriptor> getCatalog(String catalogName) throws CatalogException {
    Catalog catalog = gravitinoCatalogManager.getGravitinoCatalogInfo(catalogName);
    CatalogDescriptor descriptor =
        CatalogDescriptor.of(catalogName, Configuration.fromMap(catalog.properties()));
    return Optional.of(descriptor);
  }

  @Override
  public Set<String> listCatalogs() throws CatalogException {
    return gravitinoCatalogManager.listCatalogs();
  }

  @Override
  public boolean contains(String s) throws CatalogException {
    return false;
  }

  @Override
  public void open() throws CatalogException {}

  @Override
  public void close() throws CatalogException {}
}
