/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.TableCatalog;

/** Transform table properties between Gravitino and Spark. */
public interface PropertiesConverter {
  Map<String, String> toGravitinoTableProperties(Map<String, String> properties);

  Map<String, String> toSparkTableProperties(Map<String, String> properties);

  /** Remove 'option.' from property key name. */
  static Map<String, String> transformOptionProperties(Map<String, String> properties) {
    return properties.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> {
                  String key = entry.getKey();
                  if (key.startsWith(TableCatalog.OPTION_PREFIX)) {
                    return key.substring(TableCatalog.OPTION_PREFIX.length());
                  } else {
                    return key;
                  }
                },
                entry -> entry.getValue(),
                (existingValue, newValue) -> existingValue));
  }
}
