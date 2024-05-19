/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/** The helper class for {@link SecurableObject}. */
public class SecurableObjects {

  private static final Splitter DOT = Splitter.on('.');

  /**
   * Create the metalake {@link SecurableObject} with the given metalake name.
   *
   * @param metalake The metalake name
   * @param privileges The privileges of the metalake
   * @return The created metalake {@link SecurableObject}
   */
  public static SecurableObject ofMetalake(String metalake, List<Privilege> privileges) {
    checkName(metalake);
    checkPrivileges(privileges);

    return new SecurableObjectImpl(null, metalake, SecurableObject.Type.METALAKE, privileges);
  }

  /**
   * Create the catalog {@link SecurableObject} with the given catalog name.
   *
   * @param catalog The catalog name
   * @param privileges The privileges of the catalog
   * @return The created catalog {@link SecurableObject}
   */
  public static SecurableObject ofCatalog(String catalog, List<Privilege> privileges) {
    checkName(catalog);
    checkPrivileges(privileges);

    return new SecurableObjectImpl(null, catalog, SecurableObject.Type.CATALOG, privileges);
  }

  /**
   * Create the schema {@link SecurableObject} with the given securable catalog object and schema
   * name.
   *
   * @param catalogFullName The catalog full name
   * @param schema The schema name
   * @param privileges The privileges of the schema
   * @return The created schema {@link SecurableObject}
   */
  public static SecurableObject ofSchema(
      String catalogFullName, String schema, List<Privilege> privileges) {
    checkCatalog(catalogFullName);
    checkName(schema);
    checkPrivileges(privileges);

    return new SecurableObjectImpl(
        catalogFullName, schema, SecurableObject.Type.SCHEMA, privileges);
  }

  /**
   * Create the table {@link SecurableObject} with the given securable schema object and table name.
   *
   * @param schemaFullName The schema full name
   * @param table The table name
   * @param privileges The privileges of the table
   * @return The created table {@link SecurableObject}
   */
  public static SecurableObject ofTable(
      String schemaFullName, String table, List<Privilege> privileges) {
    checkSchema(schemaFullName);
    checkName(table);
    checkPrivileges(privileges);

    return new SecurableObjectImpl(schemaFullName, table, SecurableObject.Type.TABLE, privileges);
  }

  /**
   * Create the topic {@link SecurableObject} with the given securable schema object and topic name.
   *
   * @param schemaFullName The schema full name
   * @param topic The topic name
   * @param privileges The privileges of the topic
   * @return The created topic {@link SecurableObject}
   */
  public static SecurableObject ofTopic(
      String schemaFullName, String topic, List<Privilege> privileges) {
    checkSchema(schemaFullName);
    checkName(topic);
    checkPrivileges(privileges);

    return new SecurableObjectImpl(schemaFullName, topic, SecurableObject.Type.TOPIC, privileges);
  }

  /**
   * Create the table {@link SecurableObject} with the given securable schema object and fileset
   * name.
   *
   * @param schemaFullName The schema full name
   * @param fileset The fileset name
   * @param privileges The privileges of the fileset
   * @return The created fileset {@link SecurableObject}
   */
  public static SecurableObject ofFileset(
      String schemaFullName, String fileset, List<Privilege> privileges) {
    checkSchema(schemaFullName);
    checkName(fileset);
    checkPrivileges(privileges);

    return new SecurableObjectImpl(
        schemaFullName, fileset, SecurableObject.Type.FILESET, privileges);
  }

  /**
   * All metalakes is a special securable object .You can give the securable object the privileges
   * `CREATE METALAKE`, etc. It means that you can create any which doesn't exist. This securable
   * object is only used for metalake admin. You can't grant any privilege to this securable object.
   * You can't bind this securable object to any role, too.
   *
   * @param privileges The privileges of the all metalakes
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject ofAllMetalakes(List<Privilege> privileges) {
    checkPrivileges(privileges);

    return new SecurableObjectImpl(null, "*", SecurableObject.Type.METALAKE, privileges);
  }

  private static void checkSchema(String schemaFullName) {
    if (StringUtils.isBlank(schemaFullName)) {
      throw new IllegalArgumentException("Schema full name can't be blank");
    }

    if (DOT.splitToList(schemaFullName).size() != 2) {
      throw new IllegalArgumentException("Schema full name has a wrong format");
    }
  }

  private static void checkCatalog(String catalogFullName) {
    if (StringUtils.isBlank(catalogFullName)) {
      throw new IllegalArgumentException("Catalog full name can't be blank");
    }

    if (DOT.splitToList(catalogFullName).size() != 1) {
      throw new IllegalArgumentException("Catalog full name has a wrong format");
    }
  }

  private static class SecurableObjectImpl implements SecurableObject {

    private final String parentFullName;
    private final String name;
    private final Type type;
    private List<Privilege> privileges;

    SecurableObjectImpl(String parentFullName, String name, Type type) {
      this.parentFullName = parentFullName;
      this.name = name;
      this.type = type;
    }

    SecurableObjectImpl(String parentFullName, String name, Type type, List<Privilege> privileges) {
      this.parentFullName = parentFullName;
      this.name = name;
      this.type = type;
      this.privileges = ImmutableList.copyOf(privileges);
    }

    @Override
    public String parentFullName() {
      return parentFullName;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String fullName() {
      return toString();
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public List<Privilege> privileges() {
      return privileges;
    }

    @Override
    public int hashCode() {
      return Objects.hash(parentFullName, name, type, privileges);
    }

    @Override
    public String toString() {
      if (parentFullName != null) {
        return parentFullName + "." + name;
      } else {
        return name;
      }
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof SecurableObject)) {
        return false;
      }

      SecurableObject otherSecurableObject = (SecurableObject) other;
      return Objects.equals(parentFullName, otherSecurableObject.parentFullName())
          && Objects.equals(name, otherSecurableObject.name())
          && Objects.equals(type, otherSecurableObject.type())
          && Objects.equals(privileges, otherSecurableObject.privileges());
    }
  }

  /**
   * Create a {@link SecurableObject} from the given full name.
   *
   * @param fullName The full name of securable object.
   * @param type The securable object type.
   * @param privileges The secureable object privileges.
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject of(
      String fullName, SecurableObject.Type type, List<Privilege> privileges) {
    if ("*".equals(fullName)) {
      if (type != SecurableObject.Type.METALAKE) {
        throw new IllegalArgumentException("If securable object isn't metalake, it can't be `*`");
      }
      return SecurableObjects.ofAllMetalakes(privileges);
    }

    if (StringUtils.isBlank(fullName)) {
      throw new IllegalArgumentException("securable object full name can't be blank");
    }

    List<String> parts = DOT.splitToList(fullName);

    return SecurableObjects.of(type, parts, privileges);
  }

  /**
   * Create the {@link SecurableObject} with the given names.
   *
   * @param type The securable object type.
   * @param names The names of the securable object.
   * @param privileges The secureable object privileges.
   * @return The created {@link SecurableObject}
   */
  static SecurableObject of(
      SecurableObject.Type type, List<String> names, List<Privilege> privileges) {

    if (names == null) {
      throw new IllegalArgumentException("Cannot create a securable object with null names");
    }

    if (names.isEmpty()) {
      throw new IllegalArgumentException("Cannot create a securable object with no names");
    }

    if (type == null) {
      throw new IllegalArgumentException("Cannot create a securable object with no type");
    }

    if (names.size() > 3) {
      throw new IllegalArgumentException(
          "Cannot create a securable object with the name length which is greater than 3");
    }

    if (names.size() == 1
        && type != SecurableObject.Type.CATALOG
        && type != SecurableObject.Type.METALAKE) {
      throw new IllegalArgumentException(
          "If the length of names is 1, it must be the CATALOG or METALAKE type");
    }

    if (names.size() == 2 && type != SecurableObject.Type.SCHEMA) {
      throw new IllegalArgumentException("If the length of names is 2, it must be the SCHEMA type");
    }

    if (names.size() == 3
        && type != SecurableObject.Type.FILESET
        && type != SecurableObject.Type.TABLE
        && type != SecurableObject.Type.TOPIC) {
      throw new IllegalArgumentException(
          "If the length of names is 3, it must be FILESET, TABLE or TOPIC");
    }

    return new SecurableObjectImpl(getParentFullName(names), getLastName(names), type, privileges);
  }

  private static String getParentFullName(List<String> names) {
    if (names.size() <= 1) {
      return null;
    }

    return String.join(".", names.subList(0, names.size() - 1));
  }

  private static String getLastName(List<String> names) {
    return names.get(names.size() - 1);
  }

  private static void checkName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Cannot create a securable object with null name");
    }

    if ("*".equals(name)) {
      throw new IllegalArgumentException("Cannot create a securable object with `*` name.");
    }
  }

  private static void checkPrivileges(List<Privilege> privileges) {
    if (privileges == null || privileges.isEmpty()) {
      throw new IllegalArgumentException("Securable object should bind some privileges");
    }
  }
}
