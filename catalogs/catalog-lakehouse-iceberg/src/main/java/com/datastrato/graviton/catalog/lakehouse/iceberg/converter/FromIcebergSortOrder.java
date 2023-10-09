/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.rel.SortOrder;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.transforms.SortOrderVisitor;

/**
 * Implement iceberg sort order converter to graviton sort order.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SortOrderToSpark.java
 */
public class FromIcebergSortOrder implements SortOrderVisitor<SortOrder> {

  private final Map<Integer, String> idToName;

  public FromIcebergSortOrder(Schema schema) {
    this.idToName = schema.idToName();
  }

  @Override
  public SortOrder field(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return getSortOrder(id, direction, nullOrder);
  }

  @Override
  public SortOrder bucket(
      String sourceName, int id, int width, SortDirection direction, NullOrder nullOrder) {
    return getSortOrder(id, direction, nullOrder);
  }

  @Override
  public SortOrder truncate(
      String sourceName, int id, int width, SortDirection direction, NullOrder nullOrder) {
    return getSortOrder(id, direction, nullOrder);
  }

  @Override
  public SortOrder year(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return getSortOrder(id, direction, nullOrder);
  }

  @Override
  public SortOrder month(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return getSortOrder(id, direction, nullOrder);
  }

  private SortOrder getSortOrder(int id, SortDirection direction, NullOrder nullOrder) {
    return SortOrder.fieldSortOrder(
        new String[] {idToName.get(id)}, toGraviton(direction), toGraviton(nullOrder));
  }

  @Override
  public SortOrder day(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return getSortOrder(id, direction, nullOrder);
  }

  @Override
  public SortOrder hour(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
    return getSortOrder(id, direction, nullOrder);
  }

  private SortOrder.Direction toGraviton(SortDirection direction) {
    return direction == SortDirection.ASC ? SortOrder.Direction.ASC : SortOrder.Direction.DESC;
  }

  private SortOrder.NullOrdering toGraviton(NullOrder nullOrder) {
    return nullOrder == NullOrder.NULLS_FIRST
        ? SortOrder.NullOrdering.FIRST
        : SortOrder.NullOrdering.LAST;
  }

  /**
   * Convert Iceberg order to Graviton.
   *
   * @param sortOrder
   * @return Graviton sort order
   */
  @VisibleForTesting
  public static SortOrder[] fromSortOrder(org.apache.iceberg.SortOrder sortOrder) {
    FromIcebergSortOrder visitor = new FromIcebergSortOrder(sortOrder.schema());
    List<SortOrder> ordering = SortOrderVisitor.visit(sortOrder, visitor);
    return ordering.toArray(new SortOrder[0]);
  }
}
