/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GravitinoCatalogFactoryOptions {
  public static final String IDENTIFIER = "gravitino";

  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key("default.db")
          .stringType()
          .defaultValue("default");

  private GravitinoCatalogFactoryOptions() {}
}
