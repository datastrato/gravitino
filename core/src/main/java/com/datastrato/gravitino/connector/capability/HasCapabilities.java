/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

public interface HasCapabilities {
  default ColumnNotNull columnNotNull() {
    return ColumnNotNull.supported;
  }

  default ColumnDefaultValue columnDefaultValue() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement columnDefaultValue capability");
  }

  default CaseSensitiveOnName caseSensitiveOnName(Capability.Scope scope) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement caseSensitiveOnName capability");
  }

  default SpecificationOnName specificationOnName() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement specificationOnName");
  }

  default ManagedStorage managedStorage() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement managedStorage");
  }
}
