/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.store;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GravitinoCatalogStoreFactoryOptions {

  private GravitinoCatalogStoreFactoryOptions() {}

  public static final String GRAVITINO = "gravitino";

  public static final ConfigOption<String> METALAKE_URL =
      ConfigOptions.key("gravitino.metalake.url")
          .stringType()
          .noDefaultValue()
          .withDescription("The url of gravitino metalake");
  public static final ConfigOption<String> METALAKE_NAME =
      ConfigOptions.key("gravitino.metalake.name")
          .stringType()
          .noDefaultValue()
          .withDescription("The name of gravitino metalake");

  public static final ConfigOption<String> PROVIDER =
      ConfigOptions.key("provider")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The provider of the catalog, such as hive, hadoop, jdbc-mysql and so on");
}
