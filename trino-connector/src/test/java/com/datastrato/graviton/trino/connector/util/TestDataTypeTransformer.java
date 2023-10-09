/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.util;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_UNSUPPORTED_GRAVITON_DATATYPE;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_UNSUPPORTED_TRINO_DATATYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;

import io.substrait.type.TypeCreator;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataTypeTransformer {

  @Test
  public void testGetGravitonType() {
    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(VARCHAR, true), TypeCreator.NULLABLE.STRING);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(VARCHAR, false), TypeCreator.REQUIRED.STRING);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(INTEGER, true), TypeCreator.NULLABLE.I32);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(INTEGER, false), TypeCreator.REQUIRED.I32);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(BIGINT, true), TypeCreator.NULLABLE.I64);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(BIGINT, false), TypeCreator.REQUIRED.I64);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(DATE, true), TypeCreator.NULLABLE.DATE);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(DATE, false), TypeCreator.REQUIRED.DATE);

    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(TIMESTAMP_SECONDS, true),
        TypeCreator.NULLABLE.TIMESTAMP);
    Assertions.assertEquals(
        DataTypeTransformer.getGravitonType(TIMESTAMP_SECONDS, false),
        TypeCreator.REQUIRED.TIMESTAMP);

    try {
      DataTypeTransformer.getGravitonType(HYPER_LOG_LOG, true);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITON_UNSUPPORTED_TRINO_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }

  @Test
  public void testGetTrinoType() {
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.STRING), VARCHAR);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.STRING), VARCHAR);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I32), INTEGER);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I32), INTEGER);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.I64), BIGINT);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.I64), BIGINT);

    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.DATE), DATE);
    Assertions.assertEquals(DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.DATE), DATE);

    Assertions.assertEquals(
        DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.TIMESTAMP), TIMESTAMP_SECONDS);
    Assertions.assertEquals(
        DataTypeTransformer.getTrinoType(TypeCreator.REQUIRED.TIMESTAMP), TIMESTAMP_SECONDS);

    try {
      DataTypeTransformer.getTrinoType(TypeCreator.NULLABLE.BINARY);
    } catch (TrinoException e) {
      if (e.getErrorCode() != GRAVITON_UNSUPPORTED_GRAVITON_DATATYPE.toErrorCode()) {
        throw e;
      }
    }
  }
}
