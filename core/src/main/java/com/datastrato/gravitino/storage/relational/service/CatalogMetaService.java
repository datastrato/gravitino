/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The service class for catalog metadata. It provides the basic database operations for catalog.
 */
public class CatalogMetaService {
  private static final CatalogMetaService INSTANCE = new CatalogMetaService();

  public static CatalogMetaService getInstance() {
    return INSTANCE;
  }

  private CatalogMetaService() {}

  public CatalogPO getCatalogPOByMetalakeIdAndName(Long metalakeId, String catalogName) {
    CatalogPO catalogPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogMetaByMetalakeIdAndName(metalakeId, catalogName));

    if (catalogPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          catalogName);
    }
    return catalogPO;
  }

  public Long getCatalogIdByMetalakeIdAndName(Long metalakeId, String catalogName) {
    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));

    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          catalogName);
    }
    return catalogId;
  }

  public CatalogEntity getCatalogByIdentifier(NameIdentifier identifier) {
    NameIdentifier.checkCatalog(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.name();

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    CatalogPO catalogPO = getCatalogPOByMetalakeIdAndName(metalakeId, catalogName);

    return POConverters.fromCatalogPO(catalogPO, identifier.namespace());
  }

  public List<CatalogEntity> listCatalogsByNamespace(Namespace namespace) {
    Namespace.checkCatalog(namespace);
    String metalakeName = namespace.level(0);

    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    List<CatalogPO> catalogPOS =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByMetalakeId(metalakeId));

    return POConverters.fromCatalogPOs(catalogPOS, namespace);
  }

  public void insertCatalog(CatalogEntity catalogEntity, boolean overwrite) {
    try {
      NameIdentifier.checkCatalog(catalogEntity.nameIdentifier());

      Long metalakeId =
          MetalakeMetaService.getInstance().getMetalakeIdByName(catalogEntity.namespace().level(0));

      SessionUtils.doWithCommit(
          CatalogMetaMapper.class,
          mapper -> {
            CatalogPO po = POConverters.initializeCatalogPOWithVersion(catalogEntity, metalakeId);
            if (overwrite) {
              mapper.insertCatalogMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertCatalogMeta(po);
            }
          });
    } catch (RuntimeException re) {
      if (re.getCause() != null
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Catalog entity: %s already exists", catalogEntity.nameIdentifier()));
      }
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> CatalogEntity updateCatalog(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifier.checkCatalog(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.name();
    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    CatalogPO oldCatalogPO = getCatalogPOByMetalakeIdAndName(metalakeId, catalogName);

    CatalogEntity oldCatalogEntity =
        POConverters.fromCatalogPO(oldCatalogPO, identifier.namespace());
    CatalogEntity newEntity = (CatalogEntity) updater.apply((E) oldCatalogEntity);
    Preconditions.checkArgument(
        Objects.equals(oldCatalogEntity.id(), newEntity.id()),
        "The updated catalog entity id: %s should be same with the catalog entity id before: %s",
        newEntity.id(),
        oldCatalogEntity.id());

    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              CatalogMetaMapper.class,
              mapper ->
                  mapper.updateCatalogMeta(
                      POConverters.updateCatalogPOWithVersion(oldCatalogPO, newEntity, metalakeId),
                      oldCatalogPO));
    } catch (RuntimeException re) {
      if (re.getCause() != null
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Catalog entity: %s already exists", newEntity.nameIdentifier()));
      }
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  public boolean deleteCatalog(NameIdentifier identifier, boolean cascade) {
    NameIdentifier.checkCatalog(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.name();
    Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

    Long catalogId = getCatalogIdByMetalakeIdAndName(metalakeId, catalogName);

    if (cascade) {
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  CatalogMetaMapper.class,
                  mapper -> mapper.softDeleteCatalogMetasByCatalogId(catalogId)),
          () ->
              SessionUtils.doWithoutCommit(
                  SchemaMetaMapper.class,
                  mapper -> mapper.softDeleteSchemaMetasByCatalogId(catalogId)),
          () -> {
            // TODO We will cascade delete the metadata of sub-resources under the catalog
          });
    } else {
      List<SchemaEntity> schemaEntities =
          SchemaMetaService.getInstance()
              .listSchemasByNamespace(Namespace.ofSchema(metalakeName, catalogName));
      if (!schemaEntities.isEmpty()) {
        throw new NonEmptyEntityException(
            "Entity %s has sub-entities, you should remove sub-entities first", identifier);
      }
      SessionUtils.doWithCommit(
          CatalogMetaMapper.class, mapper -> mapper.softDeleteCatalogMetasByCatalogId(catalogId));
    }

    return true;
  }
}
