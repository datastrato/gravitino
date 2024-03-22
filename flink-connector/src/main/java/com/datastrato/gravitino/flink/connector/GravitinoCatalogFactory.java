/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector;

import com.datastrato.gravitino.flink.connector.catalog.FlinkGravitinoCatalog;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class GravitinoCatalogFactory implements CatalogFactory {
  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtil.createCatalogFactoryHelper(this, context);
    // if use this factory to create multiple catalog, we can ignore the validate
    helper.validate();

    String provider = context.getOptions().get("provider");
    return new FlinkGravitinoCatalog("", "", provider);
  }

  @Override
  public String factoryIdentifier() {
    return GravitinoCatalogFactoryOptions.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return CatalogFactory.super.requiredOptions();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return CatalogFactory.super.optionalOptions();
  }
}
