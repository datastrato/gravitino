/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.service.metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.datastrato.gravitino.iceberg.common.IcebergConfig;
import com.datastrato.gravitino.iceberg.common.IcebergConstants;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergMetricsManager {

  private MetricsReport createMetricsReport() {
    ImmutableCommitMetricsResult commitMetricsResult =
        ImmutableCommitMetricsResult.builder().build();
    MetricsReport metricsReport =
        ImmutableCommitReport.builder()
            .tableName("a")
            .snapshotId(1)
            .sequenceNumber(1)
            .operation("select")
            .commitMetrics(commitMetricsResult)
            .build();
    return metricsReport;
  }

  private MetricsReport tryGetIcebergMetrics(MemoryMetricsStore memoryMetricsStore) {
    await()
        .atMost(20, TimeUnit.SECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertTrue(memoryMetricsStore.getMetricsReport() != null));
    return memoryMetricsStore.getMetricsReport();
  }

  @Test
  void testIcebergMetricsManager() {
    IcebergConfig icebergConfig = new IcebergConfig();

    IcebergMetricsManager icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    icebergMetricsManager.start();

    MetricsReport metricsReport = createMetricsReport();
    icebergMetricsManager.recordMetric(metricsReport);
    Assertions.assertDoesNotThrow(
        () -> (DummyMetricsStore) icebergMetricsManager.getIcebergMetricsStore());
    icebergMetricsManager.close();
  }

  @Test
  void testIcebergMetricsManagerWithNotExistsStoreType() {
    IcebergConfig icebergConfig =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.ICEBERG_METRICS_STORE, "not-exists"));

    Assertions.assertThrowsExactly(
        RuntimeException.class, () -> new IcebergMetricsManager(icebergConfig));
  }

  @Test
  void testIcebergMetricsManagerWithMemoryStore() throws InterruptedException {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergConstants.ICEBERG_METRICS_STORE, MemoryMetricsStore.class.getName(), "a", "b");
    IcebergConfig icebergConfig = new IcebergConfig(properties);

    IcebergMetricsManager icebergMetricsManager = new IcebergMetricsManager(icebergConfig);
    icebergMetricsManager.start();

    MetricsReport metricsReport = createMetricsReport();
    icebergMetricsManager.recordMetric(metricsReport);
    MemoryMetricsStore memoryMetricsStore =
        (MemoryMetricsStore) icebergMetricsManager.getIcebergMetricsStore();
    Assertions.assertEquals(metricsReport, tryGetIcebergMetrics(memoryMetricsStore));
    Assertions.assertEquals(properties, memoryMetricsStore.getProperties());

    icebergMetricsManager.close();
  }
}
