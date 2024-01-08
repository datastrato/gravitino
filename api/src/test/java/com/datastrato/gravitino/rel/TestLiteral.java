/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import static com.datastrato.gravitino.rel.expressions.literals.Literals.booleanLiteral;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.byteLiteral;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.date;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.doubleLiteral;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.floatLiteral;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.integer;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.longLiteral;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.shortLiteral;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.string;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.time;
import static com.datastrato.gravitino.rel.expressions.literals.Literals.timestamp;

import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.types.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLiteral {

  @Test
  public void testLiterals() {
    Literal literal = booleanLiteral(Boolean.valueOf("true"));
    Assertions.assertEquals(literal.value(), true);
    Assertions.assertEquals(literal.dataType(), Types.BooleanType.get());

    literal = byteLiteral(Byte.valueOf("1"));
    Assertions.assertEquals(literal.value(), (byte) 1);
    Assertions.assertEquals(literal.dataType(), Types.ByteType.get());

    literal = shortLiteral(Short.valueOf("1"));
    Assertions.assertEquals(literal.value(), (short) 1);
    Assertions.assertEquals(literal.dataType(), Types.ShortType.get());

    literal = integer(Integer.valueOf("1"));
    Assertions.assertEquals(literal.value(), 1);
    Assertions.assertEquals(literal.dataType(), Types.IntegerType.get());

    literal = longLiteral(Long.valueOf("1"));
    Assertions.assertEquals(literal.value(), 1L);
    Assertions.assertEquals(literal.dataType(), Types.LongType.get());

    literal = floatLiteral(Float.valueOf("1.234"));
    Assertions.assertEquals(literal.value(), 1.234f);
    Assertions.assertEquals(literal.dataType(), Types.FloatType.get());

    literal = doubleLiteral(Double.valueOf("1.234"));
    Assertions.assertEquals(literal.value(), 1.234d);
    Assertions.assertEquals(literal.dataType(), Types.DoubleType.get());

    literal = date(LocalDate.parse("2020-01-01"));
    Assertions.assertEquals(literal.value(), LocalDate.of(2020, 1, 1));
    Assertions.assertEquals(literal.dataType(), Types.DateType.get());

    literal = time(LocalTime.parse("12:34:56"));
    Assertions.assertEquals(literal.value(), LocalTime.of(12, 34, 56));
    Assertions.assertEquals(literal.dataType(), Types.TimeType.get());

    literal = timestamp(LocalDateTime.parse("2020-01-01T12:34:56"));
    Assertions.assertEquals(literal.value(), LocalDateTime.of(2020, 1, 1, 12, 34, 56));
    Assertions.assertEquals(literal.dataType(), Types.TimestampType.withoutTimeZone());

    literal = string("hello");
    Assertions.assertEquals(literal.value(), "hello");
    Assertions.assertEquals(literal.dataType(), Types.StringType.get());

    Assertions.assertEquals(Literals.of(null, Types.NullType.get()), Literals.NULL);
  }
}
