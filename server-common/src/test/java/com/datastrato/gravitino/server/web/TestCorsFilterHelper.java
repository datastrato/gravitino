/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.Config;
import java.util.Map;
import org.eclipse.jetty.servlet.FilterHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCorsFilterHelper {

  @Test
  public void testCreateCorsFilterHolder() {
    Config config = new Config() {};
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(config, "");
    FilterHolder filterHolder = CorsFilterHelper.createCorsFilterHolder(jettyServerConfig);
    Map<String, String> parameters = filterHolder.getInitParameters();
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_ORIGINS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_ORIGINS.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_TIMING_ORIGINS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_TIMING_ORIGINS.getKey()));
    Assertions.assertEquals(
        String.valueOf(JettyServerConfig.ALLOW_CREDENTIALS.getDefaultValue()),
        parameters.get(JettyServerConfig.ALLOW_CREDENTIALS.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_HEADERS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_HEADERS.getKey()));
    Assertions.assertEquals(
        String.valueOf(JettyServerConfig.CHAIN_PREFLIGHT.getDefaultValue()),
        parameters.get(JettyServerConfig.CHAIN_PREFLIGHT.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.EXPOSED_HEADERS.getDefaultValue(),
        parameters.get(JettyServerConfig.EXPOSED_HEADERS.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_METHODS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_METHODS.getKey()));
    Assertions.assertEquals(
        String.valueOf(JettyServerConfig.PREFLIGHT_MAX_AGE_SEC.getDefaultValue()),
        parameters.get(JettyServerConfig.PREFLIGHT_MAX_AGE_SEC.getKey()));
    Assertions.assertEquals(
        "org.eclipse.jetty.servlets.CrossOriginFilter", filterHolder.getClassName());
    config.set(JettyServerConfig.ALLOWED_ORIGINS, "a");
    config.set(JettyServerConfig.ALLOWED_TIMING_ORIGINS, "b");
    config.set(JettyServerConfig.ALLOWED_HEADERS, "c");
    config.set(JettyServerConfig.ALLOWED_METHODS, "d");
    config.set(JettyServerConfig.EXPOSED_HEADERS, "e");
    config.set(JettyServerConfig.ALLOW_CREDENTIALS, false);
    config.set(JettyServerConfig.CHAIN_PREFLIGHT, false);
    config.set(JettyServerConfig.PREFLIGHT_MAX_AGE_SEC, 10);
    jettyServerConfig = JettyServerConfig.fromConfig(config, "");
    filterHolder = CorsFilterHelper.createCorsFilterHolder(jettyServerConfig);
    parameters = filterHolder.getInitParameters();
    Assertions.assertEquals("a", parameters.get(JettyServerConfig.ALLOWED_ORIGINS.getKey()));
    Assertions.assertEquals("b", parameters.get(JettyServerConfig.ALLOWED_TIMING_ORIGINS.getKey()));
    Assertions.assertEquals("false", parameters.get(JettyServerConfig.ALLOW_CREDENTIALS.getKey()));
    Assertions.assertEquals("c", parameters.get(JettyServerConfig.ALLOWED_HEADERS.getKey()));
    Assertions.assertEquals("false", parameters.get(JettyServerConfig.CHAIN_PREFLIGHT.getKey()));
    Assertions.assertEquals("e", parameters.get(JettyServerConfig.EXPOSED_HEADERS.getKey()));
    Assertions.assertEquals("d", parameters.get(JettyServerConfig.ALLOWED_METHODS.getKey()));
    Assertions.assertEquals("10", parameters.get(JettyServerConfig.PREFLIGHT_MAX_AGE_SEC.getKey()));
  }
}
