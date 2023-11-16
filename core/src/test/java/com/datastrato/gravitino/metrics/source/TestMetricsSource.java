/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metrics.source;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastrato.gravitino.metrics.MetricsSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestMetricsSource extends MetricsSource {

  private static String TEST_METRICS_SOURCE = "test";
  private static int gaugeValue = 0;
  private MetricsSystem metricsSystem;

  public TestMetricsSource() {
    super(TEST_METRICS_SOURCE);
  }

  @BeforeAll
  void init() {
    metricsSystem = new MetricsSystem();
    metricsSystem.register(this);
  }

  public void incCounter(String name) {
    getCounter(name).inc();
  }

  @Test
  void testCounter() {
    getCounter("a.b").inc();
    long v = metricsSystem.getMetricRegistry().counter(TEST_METRICS_SOURCE + ".a.b").getCount();
    Assertions.assertEquals(1, v);
    Assertions.assertEquals(1, getCounter("a.b").getCount());
  }

  private int getGaugeValue() {
    gaugeValue++;
    return gaugeValue;
  }

  @Test
  void testGauge() {
    registerGauge("a.gauge", () -> getGaugeValue());
    Integer v =
        (Integer)
            metricsSystem.getMetricRegistry().gauge(TEST_METRICS_SOURCE + ".a.gauge").getValue();
    Assertions.assertEquals(1, v.intValue());
    v =
        (Integer)
            metricsSystem.getMetricRegistry().gauge(TEST_METRICS_SOURCE + ".a.gauge").getValue();
    Assertions.assertEquals(2, v.intValue());
  }

  @Test
  void testTimer() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      Timer timer = getTimer("a.timer");
      Timer.Context context = timer.time();
      try {
        // do logic
        Thread.sleep(0);
      } finally {
        context.stop();
      }
    }
    long v =
        metricsSystem
            .getMetricRegistry()
            .timer(TEST_METRICS_SOURCE + ".a.timer")
            .getSnapshot()
            .size();
    Assertions.assertEquals(100, v);
  }

  @Test
  void testHistogram() {
    for (int i = 0; i < 100; i++) {
      Histogram histogram = getHistogram("a.histogram");
      histogram.update(i);
    }

    Snapshot snapshot =
        metricsSystem
            .getMetricRegistry()
            .histogram(TEST_METRICS_SOURCE + ".a.histogram")
            .getSnapshot();
    Assertions.assertEquals(99, snapshot.getMax());
    Assertions.assertEquals(0, snapshot.getMin());
    Assertions.assertEquals(94.0, snapshot.get95thPercentile());
    Assertions.assertEquals(100, snapshot.size());
  }
}
