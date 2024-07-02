/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.integration.test;

import com.datastrato.gravitino.iceberg.common.IcebergCatalogBackend;
import com.datastrato.gravitino.iceberg.common.IcebergConfig;
import java.util.HashMap;
import java.util.Map;

public class IcebergRESTMemoryCatalogIT extends IcebergRESTServiceIT {

  @Override
  void initEnv() {}

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.MEMORY.toString().toLowerCase());

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.CATALOG_WAREHOUSE.getKey(), "/tmp/");
    return configMap;
  }
}
