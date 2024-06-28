/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.iceberg.RESTService;
import com.datastrato.gravitino.server.authentication.ServerAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRESTServer {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRESTServer.class);

  public static final String CONF_FILE = "iceberg-rest.conf";

  private final Config serverConfig;

  private RESTService icebergRESTService;
  private GravitinoEnv gravitinoEnv;

  public IcebergRESTServer(Config config) {
    this.serverConfig = config;
    this.gravitinoEnv = GravitinoEnv.getInstance();
    this.icebergRESTService = new RESTService();
  }

  public void initialize() {
    gravitinoEnv.initialize(serverConfig, false);
    icebergRESTService.serviceInit(serverConfig.getConfigsWithPrefix("gravitino.iceberg-rest"));
    ServerAuthenticator.getInstance().initialize(serverConfig);
  }

  public void start() {
    icebergRESTService.serviceStart();
  }

  public void join() {
    icebergRESTService.join();
  }

  public void stop() throws Exception {
    icebergRESTService.serviceStop();
    LOG.info("Iceberg REST service stopped");
  }

  public static void main(String[] args) {
    LOG.info("Starting Iceberg REST Server");
    String confPath = System.getenv("GRAVITINO_TEST") == null ? "" : args[0];
    ServerConfig serverConfig = ServerConfig.loadConfig(confPath, CONF_FILE);
    IcebergRESTServer icebergRESTServer = new IcebergRESTServer(serverConfig);
    icebergRESTServer.initialize();

    try {
      icebergRESTServer.start();
    } catch (Exception e) {
      LOG.error("Error while running jettyServer", e);
      System.exit(-1);
    }
    LOG.info("Done, Iceberg REST server started.");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    // Register some clean-up tasks that need to be done before shutting down
                    Thread.sleep(serverConfig.get(ServerConfig.SERVER_SHUTDOWN_TIMEOUT));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted exception:", e);
                  } catch (Exception e) {
                    LOG.error("Error while running clean-up tasks in shutdown hook", e);
                  }
                }));
    icebergRESTServer.join();

    LOG.info("Shutting down Gravitino Server ... ");
    try {
      icebergRESTServer.stop();
      LOG.info("Gravitino Server has shut down.");
    } catch (Exception e) {
      LOG.error("Error while stopping Gravitino Server", e);
    }
  }
}
