/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.iceberg.integration.test.util;

import com.datastrato.gravitino.integration.test.util.CommandExecutor;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.util.ProcessData;
import com.datastrato.gravitino.integration.test.util.ProcessData.TypesOfData;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.Future;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class IcebergRESTServerManagerForDeploy extends IcebergRESTServerManager {

  private static final String SCRIPT_NAME = "iceberg-rest-server.sh";
  private Path icebergRESTServerHome;
  private static final String SQLITE_DRIVER_DOWNLOAD_URL =
      "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.42.0.0/sqlite-jdbc-3.42.0.0.jar";

  public IcebergRESTServerManagerForDeploy() {
    String gravitinoRootDir = System.getenv("GRAVITINO_ROOT_DIR");
    this.icebergRESTServerHome =
        Paths.get(gravitinoRootDir, "distribution", "gravitino-iceberg-rest-server");
  }

  @Override
  public Path getConfigDir() {
    return Paths.get(icebergRESTServerHome.toString(), "conf");
  }

  @Override
  public Optional<Future> doStartIcebergRESTServer() throws Exception {
    JdbcDriverDownloader.downloadJdbcDriver(
        SQLITE_DRIVER_DOWNLOAD_URL, Paths.get(icebergRESTServerHome.toString(), "libs").toString());

    String cmd = String.format("%s/bin/%s start", icebergRESTServerHome.toString(), SCRIPT_NAME);
    CommandExecutor.executeCommandLocalHost(
        cmd,
        false,
        ProcessData.TypesOfData.OUTPUT,
        ImmutableMap.of("GRAVITINO_HOME", icebergRESTServerHome.toString()));
    return Optional.empty();
  }

  @Override
  public void doStopIcebergRESTServer() {
    String cmd = String.format("%s/bin/%s stop", icebergRESTServerHome.toString(), SCRIPT_NAME);
    CommandExecutor.executeCommandLocalHost(cmd, false, TypesOfData.ERROR);
  }
}
