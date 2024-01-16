/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import com.datastrato.gravitino.integration.test.util.ITUtils;
import java.io.File;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.logging.log4j.util.Strings;

public class TrinoQueryTestTool {

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    try {
      options.addOption(
          "auto",
          true,
          "Start the test containers and gravitino server automatically, the default value is 'all'. "
              + "If the value is 'gravitino', only the gravitino server will be started automatically.");

      options.addOption(
          "gen_output",
          false,
          "Generate the output file for the test set, the default value is 'false'");

      options.addOption(
          "trino_uri", true, "URL for Trino, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "hive_uri", true, "URL for Hive, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "mysql_uri", true, "URL for MySQL, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "hdfs_uri", true, "URL for HDFS, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "postgresql_uri",
          true,
          "URL for PostgreSQL, if --auto is set to 'all', this option is ignored");

      options.addOption(
          "test_sets_dir",
          true,
          "Specify the test sets' directory, "
              + "the default value is 'integration-test/src/test/resources/trino-queries'");
      options.addOption("test_set", true, "Specify the test set name to test");
      options.addOption("tester_id", true, "Specify the tester name prefix to select to test");
      options.addOption("catalog", true, "Specify the catalog name to test");

      CommandLineParser parser = new PosixParser();
      CommandLine commandLine = parser.parse(options, args);

      boolean autoStart = true;
      boolean autoStartGravitino = true;
      if (commandLine.getOptionValue("auto") != null) {
        String auto = commandLine.getOptionValue("auto");
        if (auto.equals("all")) {
          autoStart = true;
          autoStartGravitino = true;
        } else if (auto.equals("gravitino")) {
          autoStart = false;
          autoStartGravitino = true;
        } else if (auto.equals("none")) {
          autoStart = false;
          autoStartGravitino = false;
        } else {
          System.out.println("The value of --auto must be 'all' or 'gravitino'");
          return;
        }
      }

      TrinoQueryIT.ciTestsets.clear();

      String trinoUri = commandLine.getOptionValue("trino_uri");
      TrinoQueryIT.trinoUri = Strings.isBlank(trinoUri) ? TrinoQueryIT.trinoUri : trinoUri;
      String hiveUri = commandLine.getOptionValue("hive_uri");
      TrinoQueryIT.hiveMetastoreUri =
          Strings.isBlank(hiveUri) ? TrinoQueryIT.hiveMetastoreUri : hiveUri;
      String mysqlUri = commandLine.getOptionValue("mysql_uri");
      TrinoQueryIT.mysqlUri = Strings.isBlank(mysqlUri) ? TrinoQueryIT.mysqlUri : mysqlUri;
      String hdfsUri = commandLine.getOptionValue("hdfs_uri");
      TrinoQueryIT.hdfsUri = Strings.isBlank(hdfsUri) ? TrinoQueryIT.hdfsUri : hdfsUri;
      String postgresqlUri = commandLine.getOptionValue("postgresql_uri");
      TrinoQueryIT.postgresqlUri =
          Strings.isBlank(postgresqlUri) ? TrinoQueryIT.postgresqlUri : postgresqlUri;

      String testSet = commandLine.getOptionValue("test_set");
      String testerId = commandLine.getOptionValue("tester_id", "");
      String catalog = commandLine.getOptionValue("catalog", "");
      String testSetsDir = commandLine.getOptionValue("test_sets_dir", "");

      if (testSetsDir.isEmpty()) {
        testSetsDir = TrinoQueryIT.class.getClassLoader().getResource("trino-ci-testset").getPath();
        testSetsDir = ITUtils.joinPath(testSetsDir, "testsets");
      } else {
        TrinoQueryIT.testsetsDir = testSetsDir;
      }

      String testSetDir = ITUtils.joinPath(testSetsDir, testSet);
      if (testSet != null) {
        if (!new File(testSetDir).exists()) {
          System.out.println("The test set directory " + testSetDir + " does not exist");
          System.exit(1);
        }
        if (Strings.isNotEmpty(catalog)) {
          if (!new File(ITUtils.joinPath(testSetDir, "catalog_" + catalog + "_prepare.sql"))
              .exists()) {
            System.out.println("The catalog " + catalog + " does not found in testset");
            System.exit(1);
          }
        }
        if (Strings.isNotEmpty(testerId)) {
          if (Arrays.stream(TrinoQueryIT.listDirectory(testSetDir)).noneMatch(f -> f.startsWith(testerId))) {
            System.out.println("The tester " + testerId + " does not found in testset");
            System.exit(1);
          }
        }
      }

      checkEnv();

      TrinoQueryITBase.autoStart = autoStart;
      TrinoQueryITBase.autoStartGravitino = autoStartGravitino;

      TrinoQueryIT.setup();
      TrinoQueryIT testerRunner = new TrinoQueryIT();

      if (commandLine.hasOption("gen_output")) {
        String catalogFileName = "catalog_" + catalog + "_prepare.sql";
        testerRunner.runOneTestSetAndGenOutput(testSetDir, catalogFileName, testerId);
        System.out.println("The output file is generated successfully in the path " + testSetDir);
        return;
      }

      if (testSet == null) {
        testerRunner.testSql();
      } else {
        String catalogFileName = catalog.isEmpty()? "" : "catalog_" + catalog + "_prepare.sql";
        testerRunner.testSql(testSetDir, catalogFileName, testerId);
      }
      System.out.printf("All the testers completed (%d/%d)%n",
              testerRunner.testCount.get(), testerRunner.totalCount.get());
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    } finally {
      TrinoQueryIT.cleanup();
    }
  }

  private static void checkEnv() {
    if (Strings.isEmpty(System.getenv("GRAVITINO_ROOT_DIR"))) {
      throw new RuntimeException("GRAVITINO_ROOT_DIR is not set");
    }

    if (Strings.isEmpty(System.getenv("GRAVITINO_HOME"))) {
      throw new RuntimeException("GRAVITINO_HOME is not set");
    }

    if (Strings.isEmpty(System.getenv("GRAVITINO_TEST"))) {
      throw new RuntimeException("GRAVITINO_TEST is not set, please set it to 'true'");
    }
  }
}
