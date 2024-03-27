/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/** This interface defines an entity within the Gravitino framework. */
public interface Entity extends Serializable {

  /** Enumeration defining the types of entities in the Gravitino framework. */
  @Getter
  enum EntityType {
    METALAKE("ml", 0),
    CATALOG("ca", 1),
    SCHEMA("sc", 2),
    TABLE("ta", 3),
    COLUMN("co", 4),
    FILESET("fi", 5),
    TOPIC("to", 6),

    AUDIT("au", 65534);

    // Short name can be used to identify the entity type in the logs, persistent storage, etc.
    private final String shortName;
    private final int index;

    EntityType(String shortName, int index) {
      this.shortName = shortName;
      this.index = index;
    }

    public static EntityType fromShortName(String shortName) {
      for (EntityType entityType : EntityType.values()) {
        if (entityType.shortName.equals(shortName)) {
          return entityType;
        }
      }
      throw new IllegalArgumentException("Unknown entity type: " + shortName);
    }

    /**
     * Returns the parent entity types of the given entity type. The parent entity types are the
     * entity types that are higher in the hierarchy than the given entity type. For example, the
     * parent entity types of a table are schema, catalog, and metalake.
     *
     * @param entityType The entity type for which to get the parent entity types.
     * @return The parent entity types of the given entity type.
     */
    public static List<EntityType> getParentEntityTypes(EntityType entityType) {
      switch (entityType) {
        case METALAKE:
          return ImmutableList.of();
        case CATALOG:
          return ImmutableList.of(METALAKE);
        case SCHEMA:
          return ImmutableList.of(METALAKE, CATALOG);
        case TABLE:
          return ImmutableList.of(METALAKE, CATALOG, SCHEMA);
        case FILESET:
          return ImmutableList.of(METALAKE, CATALOG, SCHEMA);
        case TOPIC:
          return ImmutableList.of(METALAKE, CATALOG, SCHEMA);
        case COLUMN:
          return ImmutableList.of(METALAKE, CATALOG, SCHEMA, TABLE);
        default:
          throw new IllegalArgumentException("Unknown entity type: " + entityType);
      }
    }
  }

  /**
   * Validates the entity by ensuring the validity of its field arguments.
   *
   * @throws IllegalArgumentException If the validation fails.
   */
  default void validate() throws IllegalArgumentException {
    fields().forEach(Field::validate);
  }

  /**
   * Retrieves the fields and their associated values of the entity.
   *
   * @return A map of Field to Object representing the entity's schema with values.
   */
  Map<Field, Object> fields();

  /**
   * Retrieves the type of the entity.
   *
   * @return The type of the entity as defined by {@link EntityType}.
   */
  EntityType type();
}
