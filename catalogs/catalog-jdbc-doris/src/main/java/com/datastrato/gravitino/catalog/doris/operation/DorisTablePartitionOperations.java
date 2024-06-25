/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.operation;

import com.datastrato.gravitino.catalog.doris.utils.DorisUtils;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTablePartitionOperations;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.NotPartitionedTableException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.partitions.ListPartition;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.Partitions;
import com.datastrato.gravitino.rel.partitions.RangePartition;
import com.datastrato.gravitino.rel.types.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public final class DorisTablePartitionOperations extends JdbcTablePartitionOperations {
  private static final String RANGE_PARTITION_PATTERN_STRING =
      "types: \\[([^\\]]+)\\]; keys: \\[([^\\]]+)\\];";
  private static final Pattern RANGE_PARTITION_PATTERN =
      Pattern.compile(RANGE_PARTITION_PATTERN_STRING);

  private final JdbcTypeConverter typeConverter;

  public DorisTablePartitionOperations(
      JdbcExceptionConverter exceptionConverter,
      DataSource dataSource,
      String databaseName,
      String tableName,
      JdbcTypeConverter typeConverter) {
    super(exceptionConverter, dataSource, databaseName, tableName);
    this.typeConverter = typeConverter;
  }

  @Override
  public String[] listPartitionNames() {
    try (Connection connection = getConnection(databaseName)) {
      getPartitionInfo(connection);
      String showPartitionsSql = String.format("SHOW PARTITIONS FROM `%s`", tableName);
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(showPartitionsSql)) {
        ImmutableList.Builder<String> partitionNames = ImmutableList.builder();
        while (result.next()) {
          partitionNames.add(result.getString("PartitionName"));
        }
        return partitionNames.build().stream().toArray(String[]::new);
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition[] listPartitions() {
    try (Connection connection = getConnection(databaseName)) {
      Transform partitionInfo = getPartitionInfo(connection);
      Map<String, Type> columnTypes = getColumnType(connection);
      String showPartitionsSql = String.format("SHOW PARTITIONS FROM `%s`", tableName);
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(showPartitionsSql)) {
        ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
        while (result.next()) {
          partitions.add(fromDorisPartition(result, partitionInfo, columnTypes));
        }
        return partitions.build().stream().toArray(Partition[]::new);
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    try (Connection connection = getConnection(databaseName)) {
      Transform partitionInfo = getPartitionInfo(connection);
      Map<String, Type> columnTypes = getColumnType(connection);
      String showPartitionsSql =
          String.format(
              "SHOW PARTITIONS FROM `%s` WHERE PartitionName = \"%s\"", tableName, partitionName);
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(showPartitionsSql)) {
        while (result.next()) {
          return fromDorisPartition(result, partitionInfo, columnTypes);
        }
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
    throw new NoSuchPartitionException("Partition %s does not exist", partitionName);
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    try (Connection connection = getConnection(databaseName)) {
      Transform partitionInfo = getPartitionInfo(connection);

      String addPartitionSqlFormat = "ALTER TABLE `%s` ADD PARTITION `%s` VALUES %s";
      String addPartitionSql;
      Partition added;

      if (partition instanceof RangePartition) {
        Preconditions.checkArgument(
            partitionInfo instanceof Transforms.RangeTransform,
            "Table %s is partitioned by list, but trying to add a range partition",
            tableName);

        RangePartition rangePartition = (RangePartition) partition;
        Literal<?> upper = rangePartition.upper();
        Literal<?> lower = rangePartition.lower();
        String partitionValues;
        if (Literals.NULL.equals(upper) && Literals.NULL.equals(lower)) {
          partitionValues = "LESS THAN MAXVALUE";
        } else if (Literals.NULL.equals(lower)) {
          partitionValues = "LESS THAN (\"" + upper.value() + "\")";
        } else if (Literals.NULL.equals(upper)) {
          partitionValues = "[(\"" + lower.value() + "\"), (MAXVALUE))";
        } else {
          partitionValues = "[(\"" + lower.value() + "\"), (\"" + upper.value() + "\"))";
        }
        addPartitionSql =
            String.format(addPartitionSqlFormat, tableName, rangePartition.name(), partitionValues);

        // The partition properties actually cannot be passed into Doris, we just return an empty
        // map instead.
        added = Partitions.range(rangePartition.name(), upper, lower, Collections.emptyMap());
      } else if (partition instanceof ListPartition) {
        Preconditions.checkArgument(
            partitionInfo instanceof Transforms.ListTransform,
            "Table %s is partitioned by range, but trying to add a list partition",
            tableName);

        ListPartition listPartition = (ListPartition) partition;
        Literal<?>[][] lists = listPartition.lists();
        ImmutableList.Builder<String> listValues = ImmutableList.builder();
        for (Literal<?>[] part : lists) {
          Preconditions.checkArgument(
              part.length == ((Transforms.ListTransform) partitionInfo).fieldNames().length,
              "The number of partitioning columns must be consistent");

          StringBuilder values = new StringBuilder();
          if (part.length > 1) {
            values
                .append("(")
                .append(
                    Arrays.stream(part)
                        .map(p -> "\"" + p.value() + "\"")
                        .collect(Collectors.joining(",")))
                .append(")");
          } else {
            values.append("\"").append(part[0].value()).append("\"");
          }
          listValues.add(values.toString());
        }
        String partitionValues =
            String.format("IN (%s)", listValues.build().stream().collect(Collectors.joining(",")));
        addPartitionSql =
            String.format(addPartitionSqlFormat, tableName, listPartition.name(), partitionValues);

        added = Partitions.list(listPartition.name(), lists, Collections.emptyMap());
      } else {
        throw new IllegalArgumentException("Unsupported partition type of Doris");
      }

      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(addPartitionSql);
        return added;
      }
    } catch (SQLException e) {
      throw exceptionConverter.toGravitinoException(e);
    }
  }

  @Override
  public boolean dropPartition(String partitionName) {
    try (Connection connection = getConnection(databaseName)) {
      getPartitionInfo(connection);
      String dropPartitionsSql =
          String.format("ALTER TABLE `%s` DROP PARTITION `%s`", tableName, partitionName);
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(dropPartitionsSql);
        return true;
      }
    } catch (SQLException e) {
      GravitinoRuntimeException exception = exceptionConverter.toGravitinoException(e);
      if (exception instanceof NoSuchPartitionException) {
        return false;
      }
      throw exception;
    }
  }

  private Transform getPartitionInfo(Connection connection) throws SQLException {
    String showCreateTableSql = String.format("SHOW CREATE TABLE `%s`", tableName);
    // Note, the connection here will be reused and should not be closed.
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(showCreateTableSql)) {
      StringBuilder createTableSql = new StringBuilder();
      while (result.next()) {
        createTableSql.append(result.getString("Create Table"));
      }
      Optional<Transform> transform =
          DorisUtils.extractPartitionInfoFromSql(createTableSql.toString());
      return transform.orElseThrow(
          () -> new NotPartitionedTableException("%s is not a partitioned table", tableName));
    }
  }

  private Partition fromDorisPartition(
      ResultSet resultSet, Transform partitionInfo, Map<String, Type> columnTypes)
      throws SQLException {
    String partitionName = resultSet.getString("PartitionName");
    String partitionKey = resultSet.getString("PartitionKey");
    String partitionValues = resultSet.getString("Range");
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.put("PartitionId", resultSet.getString("PartitionId"));
    propertiesBuilder.put("VisibleVersion", resultSet.getString("VisibleVersion"));
    propertiesBuilder.put("VisibleVersionTime", resultSet.getString("VisibleVersionTime"));
    propertiesBuilder.put("State", resultSet.getString("State"));
    propertiesBuilder.put("PartitionKey", partitionKey);
    propertiesBuilder.put("DataSize", resultSet.getString("DataSize"));
    propertiesBuilder.put("IsInMemory", resultSet.getString("IsInMemory"));
    ImmutableMap<String, String> properties = propertiesBuilder.build();

    String[] partitionKeys = partitionKey.split(", ");
    if (partitionInfo instanceof Transforms.RangeTransform) {
      if (partitionKeys.length != 1) {
        throw new UnsupportedOperationException(
            "Multi-column range partitioning in Doris is not supported yet");
      }
      Type partitionColumnType = columnTypes.get(partitionKeys[0]);
      Literal<?> lower = Literals.NULL;
      Literal<?> upper = Literals.NULL;
      Matcher matcher = RANGE_PARTITION_PATTERN.matcher(partitionValues);
      if (matcher.find()) {
        String lowerValue = matcher.group(2);
        lower = Literals.of(lowerValue, partitionColumnType);
      }
      if (matcher.find()) {
        String upperValue = matcher.group(2);
        upper = Literals.of(upperValue, partitionColumnType);
      }
      return Partitions.range(partitionName, upper, lower, properties);
    } else if (partitionInfo instanceof Transforms.ListTransform) {
      Matcher matcher = RANGE_PARTITION_PATTERN.matcher(partitionValues);
      ImmutableList.Builder<Literal<?>[]> lists = ImmutableList.builder();
      while (matcher.find()) {
        String[] values = matcher.group(2).split(", ");
        ImmutableList.Builder<Literal<?>> literValues = ImmutableList.builder();
        for (int i = 0; i < values.length; i++) {
          Type partitionColumnType = columnTypes.get(partitionKeys[i]);
          literValues.add(Literals.of(values[i], partitionColumnType));
        }
        lists.add(literValues.build().stream().toArray(Literal<?>[]::new));
      }
      return Partitions.list(
          partitionName, lists.build().stream().toArray(Literal<?>[][]::new), properties);
    } else {
      throw new NotPartitionedTableException("%s is not a partitioned table", tableName);
    }
  }

  private Map<String, Type> getColumnType(Connection connection) throws SQLException {
    // Note, the connection here will be reused and should not be closed.
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet result =
        metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, null)) {
      ImmutableMap.Builder<String, Type> columnTypes = ImmutableMap.builder();
      while (result.next()) {
        if (Objects.equals(result.getString("TABLE_NAME"), tableName)) {
          JdbcTypeConverter.JdbcTypeBean typeBean =
              new JdbcTypeConverter.JdbcTypeBean(result.getString("TYPE_NAME"));
          typeBean.setColumnSize(result.getString("COLUMN_SIZE"));
          typeBean.setScale(result.getString("DECIMAL_DIGITS"));
          Type gravitinoType = typeConverter.toGravitino(typeBean);
          String columnName = result.getString("COLUMN_NAME");
          columnTypes.put(columnName, gravitinoType);
        }
      }
      return columnTypes.build();
    }
  }
}