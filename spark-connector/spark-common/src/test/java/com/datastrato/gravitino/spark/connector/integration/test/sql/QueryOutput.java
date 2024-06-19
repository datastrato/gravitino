/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.integration.test.sql;

import lombok.Getter;

@Getter
public class QueryOutput {
  private String sql;
  private String schema;
  private String output;

  public QueryOutput(String sql, String schema, String output) {
    this.sql = sql;
    this.schema = schema;
    this.output = output;
  }

  @Override
  public String toString() {
    return "-- !query\n"
        + sql
        + "\n"
        + "-- !query schema\n"
        + schema
        + "\n"
        + "-- !query output\n"
        + output;
  }
}
