/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

public interface Capability<T, R, E extends Exception> {
  /** The scope of the capability. */
  enum Scope {
    CATALOG,
    SCHEMA,
    TABLE,
    COLUMN,
    FILESET
  }

  R apply(T arg) throws E;

  R apply(Scope scope, T arg) throws E;
}
