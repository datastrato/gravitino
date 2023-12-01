package com.datastrato.gravitino.integration.test.catalog.jdbc.mysql.service;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.meta.AuditInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.MySQLContainer;

public class MysqlService {

  private Connection connection;

  public MysqlService(MySQLContainer<?> mysqlContainer) {
    String username = mysqlContainer.getUsername();
    String password = mysqlContainer.getPassword();

    try {
      connection =
          DriverManager.getConnection(
              StringUtils.substring(
                  mysqlContainer.getJdbcUrl(), 0, mysqlContainer.getJdbcUrl().lastIndexOf("/")),
              username,
              password);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public NameIdentifier[] listSchemas(Namespace namespace) {
    List<String> databases = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        databases.add(resultSet.getString(1));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return databases.stream()
        .map(s -> NameIdentifier.of(ArrayUtils.add(namespace.levels(), s)))
        .toArray(NameIdentifier[]::new);
  }

  public JdbcSchema loadSchema(NameIdentifier schemaIdent) {
    String databaseName = schemaIdent.name();
    String query = "SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setString(1, databaseName);

      // Execute the query
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchSchemaException(
              String.format(
                  "Database %s could not be found in information_schema.SCHEMATA", databaseName));
        }
        String schemaName = resultSet.getString("SCHEMA_NAME");
        // Mysql currently only supports these two attributes
        String characterSetName = resultSet.getString("DEFAULT_CHARACTER_SET_NAME");
        String collationName = resultSet.getString("DEFAULT_COLLATION_NAME");
        return new JdbcSchema.Builder()
            .withName(schemaName)
            .withProperties(
                new HashMap<String, String>() {
                  {
                    put("CHARACTER SET", characterSetName);
                    put("COLLATE", collationName);
                  }
                })
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
      }
    } catch (final SQLException se) {
      throw new RuntimeException(se);
    }
  }

  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      // ignore
    }
  }
}
