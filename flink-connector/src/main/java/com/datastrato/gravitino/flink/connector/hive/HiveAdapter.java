/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.hive;

import com.datastrato.gravitino.flink.connector.GravitinoCatalogAdapter;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

public class HiveAdapter implements GravitinoCatalogAdapter {

  @Override
  public CatalogBaseTable createFlinkBaseTable(Table table) {
    Schema schema = toFlinkSchema(table.columns());
    List<String> partitionKey = toPartitionKey(table.partitioning());
    Map<String, String> properties = toFlinkProperties(table.properties());

    return CatalogTable.of(schema, null, partitionKey, properties);
  }

  private List<String> toPartitionKey(Transform[] partitioning) {
    return ImmutableList.of();
  }

  private Schema toFlinkSchema(Column[] columns) {
    return null;
  }

  private Map<String, String> toFlinkProperties(Map<String, String> properties) {
    return properties;
  }
}
