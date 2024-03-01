/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.utils;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import com.datastrato.gravitino.storage.relational.po.FilesetPO;
import com.datastrato.gravitino.storage.relational.po.FilesetVersionPO;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.po.SchemaPO;
import com.datastrato.gravitino.storage.relational.po.TablePO;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** POConverters is a utility class to convert PO to Base and vice versa. */
public class POConverters {

  private POConverters() {}

  /**
   * Initialize MetalakePO
   *
   * @param baseMetalake BaseMetalake object
   * @return MetalakePO object with version initialized
   */
  public static MetalakePO initializeMetalakePOWithVersion(BaseMetalake baseMetalake) {
    try {
      return new MetalakePO.Builder()
          .withMetalakeId(baseMetalake.id())
          .withMetalakeName(baseMetalake.name())
          .withMetalakeComment(baseMetalake.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.auditInfo()))
          .withSchemaVersion(
              JsonUtils.anyFieldMapper().writeValueAsString(baseMetalake.getVersion()))
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update MetalakePO version
   *
   * @param oldMetalakePO the old MetalakePO object
   * @param newMetalake the new BaseMetalake object
   * @return MetalakePO object with updated version
   */
  public static MetalakePO updateMetalakePOWithVersion(
      MetalakePO oldMetalakePO, BaseMetalake newMetalake) {
    Long lastVersion = oldMetalakePO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return new MetalakePO.Builder()
          .withMetalakeId(newMetalake.id())
          .withMetalakeName(newMetalake.name())
          .withMetalakeComment(newMetalake.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newMetalake.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newMetalake.auditInfo()))
          .withSchemaVersion(
              JsonUtils.anyFieldMapper().writeValueAsString(newMetalake.getVersion()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link MetalakePO} to {@link BaseMetalake}
   *
   * @param metalakePO MetalakePO object
   * @return BaseMetalake object from MetalakePO object
   */
  public static BaseMetalake fromMetalakePO(MetalakePO metalakePO) {
    try {
      return new BaseMetalake.Builder()
          .withId(metalakePO.getMetalakeId())
          .withName(metalakePO.getMetalakeName())
          .withComment(metalakePO.getMetalakeComment())
          .withProperties(
              JsonUtils.anyFieldMapper().readValue(metalakePO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(metalakePO.getAuditInfo(), AuditInfo.class))
          .withVersion(
              JsonUtils.anyFieldMapper()
                  .readValue(metalakePO.getSchemaVersion(), SchemaVersion.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link MetalakePO} to list of {@link BaseMetalake}
   *
   * @param metalakePOs list of MetalakePO objects
   * @return list of BaseMetalake objects from list of MetalakePO objects
   */
  public static List<BaseMetalake> fromMetalakePOs(List<MetalakePO> metalakePOs) {
    return metalakePOs.stream().map(POConverters::fromMetalakePO).collect(Collectors.toList());
  }

  /**
   * Initialize CatalogPO
   *
   * @param catalogEntity CatalogEntity object
   * @return CatalogPO object with version initialized
   */
  public static CatalogPO initializeCatalogPOWithVersion(
      CatalogEntity catalogEntity, Long metalakeId) {
    try {
      return new CatalogPO.Builder()
          .withCatalogId(catalogEntity.id())
          .withCatalogName(catalogEntity.name())
          .withMetalakeId(metalakeId)
          .withType(catalogEntity.getType().name())
          .withProvider(catalogEntity.getProvider())
          .withCatalogComment(catalogEntity.getComment())
          .withProperties(
              JsonUtils.anyFieldMapper().writeValueAsString(catalogEntity.getProperties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(catalogEntity.auditInfo()))
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update CatalogPO version
   *
   * @param oldCatalogPO the old CatalogPO object
   * @param newCatalog the new CatalogEntity object
   * @return CatalogPO object with updated version
   */
  public static CatalogPO updateCatalogPOWithVersion(
      CatalogPO oldCatalogPO, CatalogEntity newCatalog, Long metalakeId) {
    Long lastVersion = oldCatalogPO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return new CatalogPO.Builder()
          .withCatalogId(newCatalog.id())
          .withCatalogName(newCatalog.name())
          .withMetalakeId(metalakeId)
          .withType(newCatalog.getType().name())
          .withProvider(newCatalog.getProvider())
          .withCatalogComment(newCatalog.getComment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newCatalog.getProperties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newCatalog.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link CatalogPO} to {@link CatalogEntity}
   *
   * @param catalogPO CatalogPO object to be converted
   * @param namespace Namespace object to be associated with the catalog
   * @return CatalogEntity object from CatalogPO object
   */
  public static CatalogEntity fromCatalogPO(CatalogPO catalogPO, Namespace namespace) {
    try {
      return CatalogEntity.builder()
          .withId(catalogPO.getCatalogId())
          .withName(catalogPO.getCatalogName())
          .withNamespace(namespace)
          .withType(Catalog.Type.valueOf(catalogPO.getType()))
          .withProvider(catalogPO.getProvider())
          .withComment(catalogPO.getCatalogComment())
          .withProperties(
              JsonUtils.anyFieldMapper().readValue(catalogPO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(catalogPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link CatalogPO} to list of {@link CatalogEntity}
   *
   * @param catalogPOs list of CatalogPO objects
   * @param namespace Namespace object to be associated with the catalog
   * @return list of CatalogEntity objects from list of CatalogPO objects
   */
  public static List<CatalogEntity> fromCatalogPOs(
      List<CatalogPO> catalogPOs, Namespace namespace) {
    return catalogPOs.stream()
        .map(catalogPO -> POConverters.fromCatalogPO(catalogPO, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Initialize SchemaPO
   *
   * @param schemaEntity SchemaEntity object
   * @return CatalogPO object with version initialized
   */
  public static SchemaPO initializeSchemaPOWithVersion(
      SchemaEntity schemaEntity, SchemaPO.Builder builder) {
    try {
      return builder
          .withSchemaId(schemaEntity.id())
          .withSchemaName(schemaEntity.name())
          .withSchemaComment(schemaEntity.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(schemaEntity.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(schemaEntity.auditInfo()))
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update SchemaPO version
   *
   * @param oldSchemaPO the old SchemaPO object
   * @param newSchema the new SchemaEntity object
   * @return SchemaPO object with updated version
   */
  public static SchemaPO updateSchemaPOWithVersion(SchemaPO oldSchemaPO, SchemaEntity newSchema) {
    Long lastVersion = oldSchemaPO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return new SchemaPO.Builder()
          .withSchemaId(oldSchemaPO.getSchemaId())
          .withSchemaName(newSchema.name())
          .withMetalakeId(oldSchemaPO.getMetalakeId())
          .withCatalogId(oldSchemaPO.getCatalogId())
          .withSchemaComment(newSchema.comment())
          .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(newSchema.properties()))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newSchema.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link SchemaPO} to {@link SchemaEntity}
   *
   * @param schemaPO SchemaPO object to be converted
   * @param namespace Namespace object to be associated with the schema
   * @return SchemaEntity object from SchemaPO object
   */
  public static SchemaEntity fromSchemaPO(SchemaPO schemaPO, Namespace namespace) {
    try {
      return new SchemaEntity.Builder()
          .withId(schemaPO.getSchemaId())
          .withName(schemaPO.getSchemaName())
          .withNamespace(namespace)
          .withComment(schemaPO.getSchemaComment())
          .withProperties(JsonUtils.anyFieldMapper().readValue(schemaPO.getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(schemaPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link SchemaPO} to list of {@link SchemaEntity}
   *
   * @param schemaPOs list of SchemaPO objects
   * @param namespace Namespace object to be associated with the schema
   * @return list of SchemaEntity objects from list of SchemaPO objects
   */
  public static List<SchemaEntity> fromSchemaPOs(List<SchemaPO> schemaPOs, Namespace namespace) {
    return schemaPOs.stream()
        .map(schemaPO -> POConverters.fromSchemaPO(schemaPO, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Initialize TablePO
   *
   * @param tableEntity TableEntity object
   * @return TablePO object with version initialized
   */
  public static TablePO initializeTablePOWithVersion(
      TableEntity tableEntity, TablePO.Builder builder) {
    try {
      return builder
          .withTableId(tableEntity.id())
          .withTableName(tableEntity.name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(tableEntity.auditInfo()))
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update TablePO version
   *
   * @param oldTablePO the old TablePO object
   * @param newTable the new TableEntity object
   * @return TablePO object with updated version
   */
  public static TablePO updateTablePOWithVersion(TablePO oldTablePO, TableEntity newTable) {
    Long lastVersion = oldTablePO.getLastVersion();
    // Will set the version to the last version + 1 when having some fields need be multiple version
    Long nextVersion = lastVersion;
    try {
      return new TablePO.Builder()
          .withTableId(oldTablePO.getTableId())
          .withTableName(newTable.name())
          .withMetalakeId(oldTablePO.getMetalakeId())
          .withCatalogId(oldTablePO.getCatalogId())
          .withSchemaId(oldTablePO.getSchemaId())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newTable.auditInfo()))
          .withCurrentVersion(nextVersion)
          .withLastVersion(nextVersion)
          .withDeletedAt(0L)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Convert {@link TablePO} to {@link TableEntity}
   *
   * @param tablePO TablePO object to be converted
   * @param namespace Namespace object to be associated with the table
   * @return TableEntity object from TablePO object
   */
  public static TableEntity fromTablePO(TablePO tablePO, Namespace namespace) {
    try {
      return new TableEntity.Builder()
          .withId(tablePO.getTableId())
          .withName(tablePO.getTableName())
          .withNamespace(namespace)
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(tablePO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link TablePO} to list of {@link TableEntity}
   *
   * @param tablePOs list of TablePO objects
   * @param namespace Namespace object to be associated with the table
   * @return list of TableEntity objects from list of TablePO objects
   */
  public static List<TableEntity> fromTablePOs(List<TablePO> tablePOs, Namespace namespace) {
    return tablePOs.stream()
        .map(tablePO -> POConverters.fromTablePO(tablePO, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Initialize FilesetPO
   *
   * @param filesetEntity FilesetEntity object
   * @return FilesetPO object with version initialized
   */
  public static FilesetPO initializeFilesetPOWithVersion(
      FilesetEntity filesetEntity, Long metalakeId, Long catalogId, Long schemaId) {
    try {
      FilesetVersionPO filesetVersionPO =
          new FilesetVersionPO.Builder()
              .withMetalakeId(metalakeId)
              .withCatalogId(catalogId)
              .withSchemaId(schemaId)
              .withFilesetId(filesetEntity.id())
              .withVersion(1L)
              .withFilesetComment(filesetEntity.comment())
              .withStorageLocation(filesetEntity.storageLocation())
              .withProperties(
                  JsonUtils.anyFieldMapper().writeValueAsString(filesetEntity.properties()))
              .withDeletedAt(0L)
              .build();
      return new FilesetPO.Builder()
          .withFilesetId(filesetEntity.id())
          .withFilesetName(filesetEntity.name())
          .withMetalakeId(metalakeId)
          .withCatalogId(catalogId)
          .withSchemaId(schemaId)
          .withType(filesetEntity.filesetType().name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(filesetEntity.auditInfo()))
          .withCurrentVersion(1L)
          .withLastVersion(1L)
          .withDeletedAt(0L)
          .withFilesetVersionPO(filesetVersionPO)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Update FilesetPO version
   *
   * @param oldFilesetPO the old FilesetPO object
   * @param newFileset the new FilesetEntity object
   * @return FilesetPO object with updated version
   */
  public static FilesetPO updateFilesetPOWithVersion(
      FilesetPO oldFilesetPO,
      FilesetEntity newFileset,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      boolean needUpdateVersion) {
    try {
      Long lastVersion = oldFilesetPO.getLastVersion();
      Long currentVersion;
      FilesetVersionPO newFilesetVersionPO;
      // Will set the version to the last version + 1
      if (needUpdateVersion) {
        lastVersion++;
        currentVersion = lastVersion;
        newFilesetVersionPO =
            new FilesetVersionPO.Builder()
                .withMetalakeId(metalakeId)
                .withCatalogId(catalogId)
                .withSchemaId(schemaId)
                .withFilesetId(newFileset.id())
                .withVersion(currentVersion)
                .withFilesetComment(newFileset.comment())
                .withStorageLocation(newFileset.storageLocation())
                .withProperties(
                    JsonUtils.anyFieldMapper().writeValueAsString(newFileset.properties()))
                .withDeletedAt(0L)
                .build();
      } else {
        currentVersion = oldFilesetPO.getCurrentVersion();
        newFilesetVersionPO = oldFilesetPO.getFilesetVersionPO();
      }
      return new FilesetPO.Builder()
          .withFilesetId(newFileset.id())
          .withFilesetName(newFileset.name())
          .withMetalakeId(metalakeId)
          .withCatalogId(catalogId)
          .withSchemaId(schemaId)
          .withType(newFileset.filesetType().name())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newFileset.auditInfo()))
          .withCurrentVersion(currentVersion)
          .withLastVersion(lastVersion)
          .withDeletedAt(0L)
          .withFilesetVersionPO(newFilesetVersionPO)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static boolean checkFilesetVersionNeedUpdate(
      FilesetVersionPO oldFilesetVersionPO,
      FilesetEntity newFileset,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long filesetId) {
    if (!oldFilesetVersionPO.getFilesetComment().equals(newFileset.comment())
        || !oldFilesetVersionPO.getStorageLocation().equals(newFileset.storageLocation())
        || !oldFilesetVersionPO.getMetalakeId().equals(metalakeId)
        || !oldFilesetVersionPO.getCatalogId().equals(catalogId)
        || !oldFilesetVersionPO.getSchemaId().equals(schemaId)
        || !oldFilesetVersionPO.getFilesetId().equals(filesetId)) {
      return true;
    }

    try {
      Map<String, String> oldProperties =
          JsonUtils.anyFieldMapper().readValue(oldFilesetVersionPO.getProperties(), Map.class);
      return !oldProperties.equals(newFileset.properties());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert {@link FilesetPO} to {@link FilesetEntity}
   *
   * @param filesetPO FilesetPO object to be converted
   * @param namespace Namespace object to be associated with the fileset
   * @return FilesetEntity object from FilesetPO object
   */
  public static FilesetEntity fromFilesetPO(FilesetPO filesetPO, Namespace namespace) {
    try {
      return new FilesetEntity.Builder()
          .withId(filesetPO.getFilesetId())
          .withName(filesetPO.getFilesetName())
          .withNamespace(namespace)
          .withComment(filesetPO.getFilesetVersionPO().getFilesetComment())
          .withFilesetType(Fileset.Type.valueOf(filesetPO.getType()))
          .withStorageLocation(filesetPO.getFilesetVersionPO().getStorageLocation())
          .withProperties(
              JsonUtils.anyFieldMapper()
                  .readValue(filesetPO.getFilesetVersionPO().getProperties(), Map.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(filesetPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Convert list of {@link FilesetPO} to list of {@link FilesetEntity}
   *
   * @param filesetPOs list of FilesetPO objects
   * @param namespace Namespace object to be associated with the fileset
   * @return list of FilesetEntity objects from list of FilesetPO objects
   */
  public static List<FilesetEntity> fromFilesetPOs(
      List<FilesetPO> filesetPOs, Namespace namespace) {
    return filesetPOs.stream()
        .map(filesetPO -> POConverters.fromFilesetPO(filesetPO, namespace))
        .collect(Collectors.toList());
  }
}
