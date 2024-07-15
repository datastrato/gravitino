/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.apache.gravitino.storage.relational.service;

import com.apache.gravitino.Entity;
import com.apache.gravitino.HasIdentifier;
import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.exceptions.NoSuchEntityException;
import com.apache.gravitino.meta.FilesetEntity;
import com.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import com.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import com.apache.gravitino.storage.relational.po.FilesetMaxVersionPO;
import com.apache.gravitino.storage.relational.po.FilesetPO;
import com.apache.gravitino.storage.relational.utils.ExceptionUtils;
import com.apache.gravitino.storage.relational.utils.POConverters;
import com.apache.gravitino.storage.relational.utils.SessionUtils;
import com.apache.gravitino.utils.NameIdentifierUtil;
import com.apache.gravitino.utils.NamespaceUtil;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service class for fileset metadata and version info. It provides the basic database
 * operations for fileset and version info.
 */
public class FilesetMetaService {
  private static final FilesetMetaService INSTANCE = new FilesetMetaService();

  private static final Logger LOG = LoggerFactory.getLogger(FilesetMetaService.class);

  public static FilesetMetaService getInstance() {
    return INSTANCE;
  }

  private FilesetMetaService() {}

  public FilesetPO getFilesetPOBySchemaIdAndName(Long schemaId, String filesetName) {
    FilesetPO filesetPO =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetMetaBySchemaIdAndName(schemaId, filesetName));

    if (filesetPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          filesetName);
    }
    return filesetPO;
  }

  // Filset may be deleted, so the FilesetPO may be null.
  public FilesetPO getFilesetPOById(Long filesetId) {
    FilesetPO filesetPO =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class, mapper -> mapper.selectFilesetMetaById(filesetId));
    return filesetPO;
  }

  public Long getFilesetIdBySchemaIdAndName(Long schemaId, String filesetName) {
    Long filesetId =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetIdBySchemaIdAndName(schemaId, filesetName));

    if (filesetId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          filesetName);
    }
    return filesetId;
  }

  public FilesetEntity getFilesetByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkFileset(identifier);

    String filesetName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    FilesetPO filesetPO = getFilesetPOBySchemaIdAndName(schemaId, filesetName);

    return POConverters.fromFilesetPO(filesetPO, identifier.namespace());
  }

  public List<FilesetEntity> listFilesetsByNamespace(Namespace namespace) {
    NamespaceUtil.checkFileset(namespace);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(namespace);

    List<FilesetPO> filesetPOs =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class, mapper -> mapper.listFilesetPOsBySchemaId(schemaId));

    return POConverters.fromFilesetPOs(filesetPOs, namespace);
  }

  public void insertFileset(FilesetEntity filesetEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkFileset(filesetEntity.nameIdentifier());

      FilesetPO.Builder builder = FilesetPO.builder();
      fillFilesetPOBuilderParentEntityId(builder, filesetEntity.namespace());

      FilesetPO po = POConverters.initializeFilesetPOWithVersion(filesetEntity, builder);

      // insert both fileset meta table and version table
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFilesetMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertFilesetMeta(po);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetVersionMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFilesetVersionOnDuplicateKeyUpdate(po.getFilesetVersionPO());
                    } else {
                      mapper.insertFilesetVersion(po.getFilesetVersionPO());
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FILESET, filesetEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> FilesetEntity updateFileset(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkFileset(identifier);

    String filesetName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    FilesetPO oldFilesetPO = getFilesetPOBySchemaIdAndName(schemaId, filesetName);
    FilesetEntity oldFilesetEntity =
        POConverters.fromFilesetPO(oldFilesetPO, identifier.namespace());
    FilesetEntity newEntity = (FilesetEntity) updater.apply((E) oldFilesetEntity);
    Preconditions.checkArgument(
        Objects.equals(oldFilesetEntity.id(), newEntity.id()),
        "The updated fileset entity id: %s should be same with the table entity id before: %s",
        newEntity.id(),
        oldFilesetEntity.id());

    Integer updateResult;
    try {
      boolean checkNeedUpdateVersion =
          POConverters.checkFilesetVersionNeedUpdate(oldFilesetPO.getFilesetVersionPO(), newEntity);
      FilesetPO newFilesetPO =
          POConverters.updateFilesetPOWithVersion(oldFilesetPO, newEntity, checkNeedUpdateVersion);
      if (checkNeedUpdateVersion) {
        // These operations are guaranteed to be atomic by the transaction. If version info is
        // inserted successfully and the uniqueness is guaranteed by `fileset_id + version +
        // deleted_at`, it means that no other transaction has been inserted (if a uniqueness
        // conflict occurs, the transaction will be rolled back), then we can consider that the
        // fileset meta update is successful
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetVersionMapper.class,
                    mapper -> mapper.insertFilesetVersion(newFilesetPO.getFilesetVersionPO())),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.updateFilesetMeta(newFilesetPO, oldFilesetPO)));
        // we set the updateResult to 1 to indicate that the update is successful
        updateResult = 1;
      } else {
        updateResult =
            SessionUtils.doWithCommitAndFetchResult(
                FilesetMetaMapper.class,
                mapper -> mapper.updateFilesetMeta(newFilesetPO, oldFilesetPO));
      }
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FILESET, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  public boolean deleteFileset(NameIdentifier identifier) {
    NameIdentifierUtil.checkFileset(identifier);

    String filesetName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    Long filesetId = getFilesetIdBySchemaIdAndName(schemaId, filesetName);

    // We should delete meta and version info
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                FilesetMetaMapper.class,
                mapper -> mapper.softDeleteFilesetMetasByFilesetId(filesetId)),
        () ->
            SessionUtils.doWithoutCommit(
                FilesetVersionMapper.class,
                mapper -> mapper.softDeleteFilesetVersionsByFilesetId(filesetId)));

    return true;
  }

  public int deleteFilesetAndVersionMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int filesetDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FilesetMetaMapper.class,
            mapper -> {
              return mapper.deleteFilesetMetasByLegacyTimeline(legacyTimeline, limit);
            });
    int filesetVersionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FilesetVersionMapper.class,
            mapper -> {
              return mapper.deleteFilesetVersionsByLegacyTimeline(legacyTimeline, limit);
            });
    return filesetDeletedCount + filesetVersionDeletedCount;
  }

  public int deleteFilesetVersionsByRetentionCount(Long versionRetentionCount, int limit) {
    // get the current version of all filesets.
    List<FilesetMaxVersionPO> filesetCurVersions =
        SessionUtils.getWithoutCommit(
            FilesetVersionMapper.class,
            mapper -> mapper.selectFilesetVersionsByRetentionCount(versionRetentionCount));

    // soft delete old versions that are older than or equal to (currentVersion -
    // versionRetentionCount).
    int totalDeletedCount = 0;
    for (FilesetMaxVersionPO filesetCurVersion : filesetCurVersions) {
      long versionRetentionLine = filesetCurVersion.getVersion() - versionRetentionCount;
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              FilesetVersionMapper.class,
              mapper ->
                  mapper.softDeleteFilesetVersionsByRetentionLine(
                      filesetCurVersion.getFilesetId(), versionRetentionLine, limit));
      totalDeletedCount += deletedCount;

      // log the deletion by current fileset version.
      LOG.info(
          "Soft delete filesetVersions count: {} which versions are older than or equal to"
              + " versionRetentionLine: {}, the current filesetId and version is: <{}, {}>.",
          deletedCount,
          versionRetentionLine,
          filesetCurVersion.getFilesetId(),
          filesetCurVersion.getVersion());
    }
    return totalDeletedCount;
  }

  private void fillFilesetPOBuilderParentEntityId(FilesetPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkFileset(namespace);
    Long parentEntityId = null;
    for (int level = 0; level < namespace.levels().length; level++) {
      String name = namespace.level(level);
      switch (level) {
        case 0:
          parentEntityId = MetalakeMetaService.getInstance().getMetalakeIdByName(name);
          builder.withMetalakeId(parentEntityId);
          continue;
        case 1:
          parentEntityId =
              CatalogMetaService.getInstance()
                  .getCatalogIdByMetalakeIdAndName(parentEntityId, name);
          builder.withCatalogId(parentEntityId);
          continue;
        case 2:
          parentEntityId =
              SchemaMetaService.getInstance().getSchemaIdByCatalogIdAndName(parentEntityId, name);
          builder.withSchemaId(parentEntityId);
          break;
      }
    }
  }
}
