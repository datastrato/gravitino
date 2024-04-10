/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.buildSchemaChange;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.TableOpsUtils.fieldName;
import static com.datastrato.gravitino.rel.TableChange.ColumnPosition.after;
import static com.datastrato.gravitino.rel.TableChange.ColumnPosition.defaultPos;
import static com.datastrato.gravitino.rel.TableChange.addColumn;
import static com.datastrato.gravitino.rel.TableChange.addIndex;
import static com.datastrato.gravitino.rel.TableChange.deleteColumn;
import static com.datastrato.gravitino.rel.TableChange.deleteIndex;
import static com.datastrato.gravitino.rel.TableChange.removeProperty;
import static com.datastrato.gravitino.rel.TableChange.rename;
import static com.datastrato.gravitino.rel.TableChange.renameColumn;
import static com.datastrato.gravitino.rel.TableChange.setProperty;
import static com.datastrato.gravitino.rel.TableChange.updateColumnAutoIncrement;
import static com.datastrato.gravitino.rel.TableChange.updateColumnComment;
import static com.datastrato.gravitino.rel.TableChange.updateColumnDefaultValue;
import static com.datastrato.gravitino.rel.TableChange.updateColumnNullability;
import static com.datastrato.gravitino.rel.TableChange.updateColumnPosition;
import static com.datastrato.gravitino.rel.TableChange.updateColumnType;
import static com.datastrato.gravitino.rel.TableChange.updateComment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.indexes.Index.IndexType;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.rel.types.Types.DoubleType;
import com.datastrato.gravitino.rel.types.Types.FloatType;
import com.datastrato.gravitino.rel.types.Types.IntegerType;
import com.datastrato.gravitino.rel.types.Types.ListType;
import com.datastrato.gravitino.rel.types.Types.MapType;
import com.datastrato.gravitino.rel.types.Types.StringType;
import java.util.Arrays;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaChange.AddColumn;
import org.apache.paimon.schema.SchemaChange.DropColumn;
import org.apache.paimon.schema.SchemaChange.Move.MoveType;
import org.apache.paimon.schema.SchemaChange.RemoveOption;
import org.apache.paimon.schema.SchemaChange.RenameColumn;
import org.apache.paimon.schema.SchemaChange.SetOption;
import org.apache.paimon.schema.SchemaChange.UpdateColumnComment;
import org.apache.paimon.schema.SchemaChange.UpdateColumnNullability;
import org.apache.paimon.schema.SchemaChange.UpdateColumnPosition;
import org.apache.paimon.schema.SchemaChange.UpdateColumnType;
import org.apache.paimon.schema.SchemaChange.UpdateComment;
import org.apache.paimon.types.DataTypeRoot;
import org.junit.jupiter.api.Test;

/** Tests for {@link TableOpsUtils}. */
public class TestTableOpsUtils {

  @Test
  void testBuildSchemaChange() {
    // Test supported table changes.
    assertTableChange(
        addColumn(
            fieldName("col_1"),
            IntegerType.get(),
            AddColumn.class.getSimpleName(),
            TableChange.ColumnPosition.first(),
            false,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_1", addColumn.fieldName());
          assertEquals(DataTypeRoot.INTEGER, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNotNull(addColumn.move());
          assertEquals(MoveType.FIRST, addColumn.move().type());
          assertEquals("col_1", addColumn.move().fieldName());
          assertNull(addColumn.move().referenceFieldName());
          assertFalse(addColumn.dataType().isNullable());
        });
    assertTableChange(
        addColumn(
            fieldName("col_2"),
            FloatType.get(),
            AddColumn.class.getSimpleName(),
            after("col_1"),
            true,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_2", addColumn.fieldName());
          assertEquals(DataTypeRoot.FLOAT, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNotNull(addColumn.move());
          assertEquals(MoveType.AFTER, addColumn.move().type());
          assertEquals("col_2", addColumn.move().fieldName());
          assertEquals("col_1", addColumn.move().referenceFieldName());
          assertTrue(addColumn.dataType().isNullable());
        });
    assertTableChange(
        addColumn(
            fieldName("col_3"),
            ListType.of(StringType.get(), false),
            AddColumn.class.getSimpleName(),
            defaultPos(),
            false,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_3", addColumn.fieldName());
          assertEquals(DataTypeRoot.ARRAY, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNull(addColumn.move());
          assertFalse(addColumn.dataType().isNullable());
        });
    assertTableChange(
        addColumn(
            fieldName("col_4"),
            MapType.of(StringType.get(), IntegerType.get(), true),
            AddColumn.class.getSimpleName(),
            null,
            false,
            false),
        AddColumn.class,
        schemaChange -> {
          AddColumn addColumn = (AddColumn) schemaChange;
          assertEquals("col_4", addColumn.fieldName());
          assertEquals(DataTypeRoot.MAP, addColumn.dataType().getTypeRoot());
          assertEquals(AddColumn.class.getSimpleName(), addColumn.description());
          assertNull(addColumn.move());
          assertFalse(addColumn.dataType().isNullable());
        });
    assertTableChange(
        updateColumnComment(fieldName("col_1"), UpdateColumnComment.class.getSimpleName()),
        UpdateColumnComment.class,
        schemaChange -> {
          UpdateColumnComment updateColumnComment = (UpdateColumnComment) schemaChange;
          assertEquals("col_1", fieldName(updateColumnComment.fieldNames()));
          assertEquals(
              UpdateColumnComment.class.getSimpleName(), updateColumnComment.newDescription());
        });
    assertTableChange(
        updateColumnNullability(fieldName("col_2"), false),
        UpdateColumnNullability.class,
        schemaChange -> {
          UpdateColumnNullability updateColumnNullability = (UpdateColumnNullability) schemaChange;
          assertEquals("col_2", fieldName(updateColumnNullability.fieldNames()));
          assertFalse(updateColumnNullability.newNullability());
        });
    assertTableChange(
        updateColumnPosition(fieldName("col_3"), after("col_1")),
        UpdateColumnPosition.class,
        schemaChange -> {
          UpdateColumnPosition updateColumnPosition = (UpdateColumnPosition) schemaChange;
          assertEquals("col_3", updateColumnPosition.move().fieldName());
          assertEquals("col_1", updateColumnPosition.move().referenceFieldName());
        });
    assertTableChange(
        updateColumnType(fieldName("col_4"), DoubleType.get()),
        UpdateColumnType.class,
        schemaChange -> {
          UpdateColumnType updateColumnType = (UpdateColumnType) schemaChange;
          assertEquals("col_4", updateColumnType.fieldName());
          assertEquals(DataTypeRoot.DOUBLE, updateColumnType.newDataType().getTypeRoot());
        });
    assertTableChange(
        renameColumn(fieldName("col_1"), "col_5"),
        RenameColumn.class,
        schemaChange -> {
          RenameColumn renameColumn = (RenameColumn) schemaChange;
          assertEquals("col_1", renameColumn.fieldName());
          assertEquals("col_5", renameColumn.newName());
        });
    assertTableChange(
        deleteColumn(fieldName("col_2"), true),
        DropColumn.class,
        schemaChange -> {
          DropColumn dropColumn = (DropColumn) schemaChange;
          assertEquals("col_2", dropColumn.fieldName());
        });
    assertTableChange(
        updateComment(UpdateComment.class.getSimpleName()),
        UpdateComment.class,
        schemaChange -> {
          UpdateComment updateComment = (UpdateComment) schemaChange;
          assertEquals(UpdateComment.class.getSimpleName(), updateComment.comment());
        });
    assertTableChange(
        setProperty("prop_k1", "prop_v1"),
        SetOption.class,
        schemaChange -> {
          SetOption setOption = (SetOption) schemaChange;
          assertEquals("prop_k1", setOption.key());
          assertEquals("prop_v1", setOption.value());
        });
    assertTableChange(
        removeProperty("prop_k1"),
        RemoveOption.class,
        schemaChange -> {
          RemoveOption removeOption = (RemoveOption) schemaChange;
          assertEquals("prop_k1", removeOption.key());
        });
    // Test UnsupportedOperationException with AddIndex, DeleteIndex, RenameTable,
    // UpdateColumnAutoIncrement, UpdateColumnDefaultValue.
    Arrays.asList(
            addIndex(IndexType.UNIQUE_KEY, "uk", new String[][] {{"col_5"}}),
            deleteIndex("uk", true),
            rename("tb_1"),
            updateColumnAutoIncrement(fieldName("col_5"), true),
            updateColumnDefaultValue(
                fieldName("col_5"), Literals.of("default", Types.VarCharType.of(255))))
        .forEach(this::assertUnsupportedTableChange);
    // Test IllegalArgumentException with AddColumn default value and auto increment.
    Arrays.asList(
            Pair.of(
                addColumn(
                    new String[] {"col_1", "col_6"},
                    IntegerType.get(),
                    AddColumn.class.getSimpleName(),
                    TableChange.ColumnPosition.first(),
                    false,
                    false),
                "Paimon does not support update non-primitive type column. Illegal column: col_1.col_6."),
            Pair.of(
                addColumn(
                    fieldName("col_1"),
                    IntegerType.get(),
                    AddColumn.class.getSimpleName(),
                    TableChange.ColumnPosition.first(),
                    false,
                    false,
                    Literals.of("default", Types.StringType.get())),
                "Paimon does not support column default value. Illegal column: col_1."),
            Pair.of(
                addColumn(
                    fieldName("col_1"),
                    IntegerType.get(),
                    AddColumn.class.getSimpleName(),
                    TableChange.ColumnPosition.first(),
                    false,
                    true),
                "Paimon does not support auto increment column. Illegal column: col_1."))
        .forEach(this::assertIllegalTableChange);
  }

  private void assertTableChange(
      TableChange tableChange, Class<?> expected, Consumer<SchemaChange> consumer) {
    SchemaChange schemaChange = buildSchemaChange(tableChange);
    assertEquals(expected, schemaChange.getClass());
    consumer.accept(schemaChange);
  }

  private void assertUnsupportedTableChange(TableChange tableChange) {
    UnsupportedOperationException exception =
        assertThrowsExactly(
            UnsupportedOperationException.class, () -> buildSchemaChange(tableChange));
    assertEquals(
        String.format(
            "Paimon does not support %s table change.", tableChange.getClass().getSimpleName()),
        exception.getMessage());
  }

  private void assertIllegalTableChange(Pair<TableChange, String> tableChange) {
    IllegalArgumentException exception =
        assertThrowsExactly(
            IllegalArgumentException.class, () -> buildSchemaChange(tableChange.getKey()));
    assertEquals(tableChange.getValue(), exception.getMessage());
  }
}
