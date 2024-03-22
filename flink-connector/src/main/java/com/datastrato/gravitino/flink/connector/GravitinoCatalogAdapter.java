/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector;

import com.datastrato.gravitino.rel.Table;
import org.apache.flink.table.catalog.CatalogBaseTable;

public interface GravitinoCatalogAdapter {

  public CatalogBaseTable createFlinkBaseTable(Table table);
}
