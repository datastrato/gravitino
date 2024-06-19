/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.integration.test.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.HiveResult;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.JavaConverters;

public class SQLQueryTestHelper {

  private static final String notIncludedMsg = "[not included in comparison]";
  private static final String clsName = SQLQueryTestHelper.class.getCanonicalName();
  private static final String emptySchema = new StructType().catalogString();

  private static String replaceNotIncludedMsg(String line) {
    line =
        line.replaceAll("#\\d+", "#x")
            .replaceAll("plan_id=\\d+", "plan_id=x")
            .replaceAll(
                "Location.*" + clsName + "/", "Location " + notIncludedMsg + "/{warehouse_dir}/")
            .replaceAll("file:[^\\s,]*" + clsName, "file:" + notIncludedMsg + "/{warehouse_dir}")
            .replaceAll("Created By.*", "Created By " + notIncludedMsg)
            .replaceAll("Created Time.*", "Created Time " + notIncludedMsg)
            .replaceAll("Last Access.*", "Last Access " + notIncludedMsg)
            .replaceAll("Partition Statistics\t\\d+", "Partition Statistics\t" + notIncludedMsg)
            .replaceAll("\\s+$", "")
            .replaceAll("\\*\\(\\d+\\) ", "*");
    return line;
  }

  public static Pair<String, List<String>> getNormalizedResult(SparkSession session, String sql) {
    Dataset<Row> df = session.sql(sql);
    String schema = df.schema().catalogString();
    List<String> answer =
        SQLExecution.withNewExecutionId(
            df.queryExecution(),
            Option.apply(""),
            () ->
                JavaConverters.seqAsJavaList(
                        HiveResult.hiveResultString(df.queryExecution().executedPlan()))
                    .stream()
                    .map(s -> replaceNotIncludedMsg(s))
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList()));

    Collections.sort(answer);

    return Pair.of(schema, answer);
  }

  // different Spark version may produce different exceptions, so here just produce
  // [SPARK_EXCEPTION]
  public static Pair<String, List<String>> handleExceptions(
      Supplier<Pair<String, List<String>>> result) {
    try {
      return result.get();
    } catch (Throwable e) {
      return Pair.of(emptySchema, Arrays.asList("[SPARK_EXCEPTION]"));
    }
  }
}
