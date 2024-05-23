/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHivePropertiesConverter {

  private static final HivePropertiesConverter CONVERTER = HivePropertiesConverter.INSTANCE;

  @Test
  public void testToGravitinoCatalogProperties() {
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                "hive-conf-dir", "src/test/resources/flink-tests", "flink.bypass.key", "value"));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);
    Assertions.assertEquals(3, properties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests", properties.get("flink.bypass.hive-conf-dir"));
    Assertions.assertEquals("value", properties.get("flink.bypass.key"));
    Assertions.assertEquals("thrift://127.0.0.1:9083", properties.get("metastore.uris"));
    Assertions.assertEquals("false", "hive.metastore.sasl.enabled");
    Assertions.assertEquals("simple", "hadoop.security.authentication");
    Assertions.assertEquals("hdfs://tmp", properties.get("hive.metastore.warehouse.dir"));
  }

  @Test
  public void testToFlinkCatalogProperties() {
    Map<String, String> catalogProperties =
        ImmutableMap.of("flink.bypass.key", "value", "metastore.uris", "thrift://xxx");
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(2, flinkCatalogProperties.size());
    Assertions.assertEquals("value", flinkCatalogProperties.get("key"));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactory.IDENTIFIER, flinkCatalogProperties.get("type"));
  }
}
