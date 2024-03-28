/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

import com.datastrato.gravitino.rel.Column;
import com.google.common.base.Preconditions;

/** A capability indicating whether a column can be defined as NOT NULL. */
public interface ColumnNotNull extends Capability<Column[], Void, IllegalArgumentException> {
  ColumnNotNullCapability supported = new ColumnNotNullCapability(true, null);

  static ColumnNotNullCapability unsupported(String unsupportedReason) {
    return new ColumnNotNullCapability(false, unsupportedReason);
  }

  final class ColumnNotNullCapability implements ColumnNotNull {

    private final boolean supportsNotNull;
    private final String unsupportedReason;

    private ColumnNotNullCapability(boolean supportsNotNull, String unsupportedReason) {
      Preconditions.checkArgument(
          supportsNotNull || unsupportedReason != null,
          "unsupportedReason is required when supportsNotNull is false");
      this.supportsNotNull = supportsNotNull;
      this.unsupportedReason = unsupportedReason;
    }

    @Override
    public Void apply(Column[] columns) throws IllegalArgumentException {
      if (supportsNotNull) {
        return null;
      }

      for (Column column : columns) {
        Preconditions.checkArgument(
            column.nullable(), unsupportedReason + " Illegal column " + column.name());
      }
      return null;
    }

    @Override
    public Void apply(Scope scope, Column[] columns) throws IllegalArgumentException {
      Preconditions.checkArgument(scope.equals(Scope.COLUMN), "scope must be COLUMN");
      return apply(columns);
    }
  }
}
