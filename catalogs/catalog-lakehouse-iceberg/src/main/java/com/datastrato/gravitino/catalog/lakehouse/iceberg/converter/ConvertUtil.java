/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import java.util.Arrays;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class ConvertUtil {

  /**
   * Convert the Iceberg Table to the corresponding schema information in the Iceberg.
   *
   * @param icebergTable Iceberg table.
   * @return Iceberg schema.
   */
  public static Schema toIcebergSchema(IcebergTable icebergTable) {
    com.datastrato.gravitino.rel.types.Types.StructType gravitinoStructType =
        toGravitinoStructType(icebergTable);
    Type converted =
        ToIcebergTypeVisitor.visit(gravitinoStructType, new ToIcebergType(gravitinoStructType));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert the Gravitino StructType to the corresponding schema information in the Iceberg.
   *
   * @param gravitinoType Gravitino StructType
   * @return Iceberg schema.
   */
  public static Schema toIcebergSchema(
      com.datastrato.gravitino.rel.types.Types.StructType gravitinoType) {
    Type converted = ToIcebergTypeVisitor.visit(gravitinoType, new ToIcebergType(gravitinoType));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Convert the Gravitino type to the Iceberg type.
   *
   * @param nullable Whether the field is nullable.
   * @param gravitinoType Gravitino type.
   * @return Iceberg type.
   */
  public static Type toIcebergType(
      boolean nullable, com.datastrato.gravitino.rel.types.Type gravitinoType) {
    return ToIcebergTypeVisitor.visit(gravitinoType, new ToIcebergType(nullable));
  }

  /**
   * Convert the nested type of Iceberg to the type of gravitino.
   *
   * @param type Iceberg type of field.
   * @return
   */
  public static com.datastrato.gravitino.rel.types.Type formIcebergType(Type type) {
    return TypeUtil.visit(type, new FromIcebergType());
  }

  /**
   * Convert the nested field of Iceberg to the Iceberg column.
   *
   * @param nestedField Iceberg nested field.
   * @return Gravitino iceberg column
   */
  public static IcebergColumn fromNestedField(Types.NestedField nestedField) {
    return new IcebergColumn.Builder()
        .withId(nestedField.fieldId())
        .withName(nestedField.name())
        .withNullable(nestedField.isOptional())
        .withComment(nestedField.doc())
        .withType(ConvertUtil.formIcebergType(nestedField.type()))
        .build();
  }

  /**
   * Convert the Gravitino field to the Iceberg Gravitino column
   *
   * @param field Gravitino field.
   * @param id Gravitino field id.
   * @return Gravitino iceberg table column
   */
  public static IcebergColumn fromGravitinoField(
      com.datastrato.gravitino.rel.types.Types.StructType.Field field, int id) {
    return new IcebergColumn.Builder()
        .withId(id)
        .withName(field.name())
        .withNullable(field.nullable())
        .withComment(field.comment())
        .withType(field.type())
        .build();
  }

  /**
   * Convert the Gravitino iceberg table to the Gravitino StructType
   *
   * @param icebergTable Gravitino iceberg table
   * @return Gravitino StructType
   */
  public static com.datastrato.gravitino.rel.types.Types.StructType toGravitinoStructType(
      IcebergTable icebergTable) {
    com.datastrato.gravitino.rel.types.Types.StructType.Field[] fields =
        Arrays.stream(icebergTable.columns())
            .map(
                column ->
                    com.datastrato.gravitino.rel.types.Types.StructType.Field.of(
                        column.name(), column.dataType(), column.nullable(), column.comment()))
            .toArray(com.datastrato.gravitino.rel.types.Types.StructType.Field[]::new);
    return com.datastrato.gravitino.rel.types.Types.StructType.of(fields);
  }
}
