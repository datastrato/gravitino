/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.integration.test;

import static com.datastrato.gravitino.catalog.mysql.operation.MysqlDatabaseOperations.SYS_MYSQL_DATABASE_NAMES;

import com.datastrato.gravitino.utils.RandomNameUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class TestMysqlDatabaseOperations extends TestMysqlAbstractIT {

  @Test
  public void testBaseOperationDatabase() {
    String databaseName = RandomNameUtils.genRandomName("ct_db");
    Map<String, String> properties = new HashMap<>();
    // Mysql database creation does not support incoming comments.
    String comment = null;
    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    SYS_MYSQL_DATABASE_NAMES.forEach(
        sysMysqlDatabaseName -> Assertions.assertFalse(databases.contains(sysMysqlDatabaseName)));
    testBaseOperation(databaseName, properties, comment);
    testDropDatabase(databaseName);
  }

  @Test
  public void testDropDatabaseWithSqlInjection() {
    String databaseName = RandomNameUtils.genRandomName("ct_db");

    // testDropDatabase should throw an exception with string that might contain SQL injection
    String sqlInjection = databaseName + "`; DROP TABLE important_table; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          testDropDatabase(sqlInjection);
        });

    // testDropDatabase should throw an exception with string that might contain SQL injection
    String sqlInjection1 = databaseName + "`; SLEEP(10); -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          testDropDatabase(sqlInjection1);
        });

    // testDropDatabase should throw an exception with string that might contain SQL injection
    String sqlInjection2 =
        databaseName + "`; UPDATE Users SET password = 'newpassword' WHERE username = 'admin'; -- ";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          testDropDatabase(sqlInjection2);
        });

    // testDropDatabase should throw an exception with input that has more than 65 characters
    String invalidInput = StringUtils.repeat("a", 65);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          testDropDatabase(invalidInput);
        });
  }
}
