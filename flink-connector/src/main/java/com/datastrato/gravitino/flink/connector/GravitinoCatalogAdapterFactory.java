/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector;

import com.datastrato.gravitino.flink.connector.hive.HiveAdapter;
import java.util.Locale;

public class GravitinoCatalogAdapterFactory {
  public static GravitinoCatalogAdapter createGravitinoAdaptor(String provider) {
    switch (provider.toLowerCase(Locale.ROOT)) {
      case "hive":
        return new HiveAdapter();
      default:
        throw new RuntimeException(String.format("Provider:%s is not supported yet", provider));
    }
  }
}
