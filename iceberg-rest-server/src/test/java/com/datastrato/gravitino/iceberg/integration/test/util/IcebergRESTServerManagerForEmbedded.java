/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.integration.test.util;

import com.datastrato.gravitino.server.IcebergRESTServer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;

public class IcebergRESTServerManagerForEmbedded extends IcebergRESTServerManager {

  private File mockConfDir;
  private final ExecutorService executor;

  public IcebergRESTServerManagerForEmbedded() {
    try {
      this.mockConfDir = Files.createTempDirectory("MiniIcebergRESTServer").toFile();
      LOG.info("config dir:{}", mockConfDir.getAbsolutePath());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mockConfDir.mkdirs();
    this.executor = Executors.newSingleThreadExecutor();
  }

  @Override
  public Path getConfigDir() {
    return mockConfDir.toPath();
  }

  @Override
  public Optional<Future> doStartIcebergRESTServer() {
    Future<?> future =
        executor.submit(
            () -> {
              try {
                IcebergRESTServer.main(
                    new String[] {
                      Paths.get(mockConfDir.getAbsolutePath(), IcebergRESTServer.CONF_FILE)
                          .toString()
                    });
              } catch (Exception e) {
                LOG.error("Exception in startup mini IcebergRESTServer ", e);
                throw new RuntimeException(e);
              }
            });

    return Optional.of(future);
  }

  @Override
  public void doStopIcebergRESTServer() {
    executor.shutdownNow();
    try {
      FileUtils.deleteDirectory(mockConfDir);
    } catch (Exception e) {
      // Ignore
    }
  }
}
