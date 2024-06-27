/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.TopicEntity;
import com.datastrato.gravitino.storage.relational.mapper.TopicMetaMapper;
import com.datastrato.gravitino.storage.relational.po.TopicPO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.datastrato.gravitino.utils.NameIdentifierUtil;
import com.datastrato.gravitino.utils.NamespaceUtil;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.commons.lang3.ArrayUtils;

/**
 * The service class for topic metadata. It provides the basic database operations for topic
 * metadata.
 */
public class TopicMetaService {
  private static final TopicMetaService INSTANCE = new TopicMetaService();

  public static TopicMetaService getInstance() {
    return INSTANCE;
  }

  private TopicMetaService() {}

  public void insertTopic(TopicEntity topicEntity, boolean overwrite) {
    try {
      NameIdentifierUtil.checkTopic(topicEntity.nameIdentifier());

      TopicPO.Builder builder = TopicPO.builder();
      fillTopicPOBuilderParentEntityId(builder, topicEntity.namespace());

      SessionUtils.doWithCommit(
          TopicMetaMapper.class,
          mapper -> {
            TopicPO po = POConverters.initializeTopicPOWithVersion(topicEntity, builder);
            if (overwrite) {
              mapper.insertTopicMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertTopicMeta(po);
            }
          });
      // TODO: insert topic dataLayout version after supporting it
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TOPIC, topicEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public List<TopicEntity> listTopicsByNamespace(Namespace namespace) {
    NamespaceUtil.checkTopic(namespace);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(namespace);

    List<TopicPO> topicPOs =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class, mapper -> mapper.listTopicPOsBySchemaId(schemaId));

    return POConverters.fromTopicPOs(topicPOs, namespace);
  }

  public <E extends Entity & HasIdentifier> TopicEntity updateTopic(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkTopic(ident);

    String topicName = ident.name();

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(ident.namespace());

    TopicPO oldTopicPO = getTopicPOBySchemaIdAndName(schemaId, topicName);
    TopicEntity oldTopicEntity = POConverters.fromTopicPO(oldTopicPO, ident.namespace());
    TopicEntity newEntity = (TopicEntity) updater.apply((E) oldTopicEntity);
    Preconditions.checkArgument(
        Objects.equals(oldTopicEntity.id(), newEntity.id()),
        "The updated topic entity id: %s should be same with the topic entity id before: %s",
        newEntity.id(),
        oldTopicEntity.id());

    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              TopicMetaMapper.class,
              mapper ->
                  mapper.updateTopicMeta(
                      POConverters.updateTopicPOWithVersion(oldTopicPO, newEntity), oldTopicPO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.TOPIC, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + ident);
    }
  }

  private TopicPO getTopicPOBySchemaIdAndName(Long schemaId, String topicName) {
    TopicPO topicPO =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class,
            mapper -> mapper.selectTopicMetaBySchemaIdAndName(schemaId, topicName));

    if (topicPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TOPIC.name().toLowerCase(),
          topicName);
    }
    return topicPO;
  }

  // Topic may be deleted, so the TopicPO may be null.
  public TopicPO getTopicPOById(Long topicId) {
    TopicPO topicPO =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class, mapper -> mapper.selectTopicMetaById(topicId));
    return topicPO;
  }

  private void fillTopicPOBuilderParentEntityId(TopicPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkTopic(namespace);
    Long entityId;

    for (int level = 0; level < namespace.levels().length; level++) {
      String[] levels = ArrayUtils.subarray(namespace.levels(), 0, level + 1);
      NameIdentifier nameIdentifier = NameIdentifier.of(levels);
      switch (level) {
        case 0:
          entityId =
              MetalakeMetaService.getInstance().getMetalakeIdByNameIdentifier(nameIdentifier);
          builder.withMetalakeId(entityId);
          break;
        case 1:
          entityId = CatalogMetaService.getInstance().getCatalogIdByNameIdentifier(nameIdentifier);
          builder.withCatalogId(entityId);
          break;
        case 2:
          entityId = SchemaMetaService.getInstance().getSchemaIdByNameIdentifier(nameIdentifier);
          builder.withSchemaId(entityId);
          break;
      }
    }
  }

  public TopicEntity getTopicByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkTopic(identifier);

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    TopicPO topicPO = getTopicPOBySchemaIdAndName(schemaId, identifier.name());

    return POConverters.fromTopicPO(topicPO, identifier.namespace());
  }

  public boolean deleteTopic(NameIdentifier identifier) {
    NameIdentifierUtil.checkTopic(identifier);

    Long topicId = getTopicIdByNameIdentifier(identifier);

    SessionUtils.doWithCommit(
        TopicMetaMapper.class, mapper -> mapper.softDeleteTopicMetasByTopicId(topicId));

    return true;
  }

  public int deleteTopicMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        TopicMetaMapper.class,
        mapper -> {
          return mapper.deleteTopicMetasByLegacyTimeline(legacyTimeline, limit);
        });
  }

  public Long getTopicIdByNameIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkTopic(identifier);

    return IdNameMappingService.getInstance()
        .get(
            identifier,
            ident -> {
              Long schemaId =
                  CommonMetaService.getInstance().getParentEntityIdByNamespace(ident.namespace());
              return getTopicIdBySchemaIdAndName(schemaId, ident.name());
            });
  }

  private Long getTopicIdBySchemaIdAndName(Long schemaId, String topicName) {
    Long topicId =
        SessionUtils.getWithoutCommit(
            TopicMetaMapper.class,
            mapper -> mapper.selectTopicIdBySchemaIdAndName(schemaId, topicName));

    if (topicId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TOPIC.name().toLowerCase(),
          topicName);
    }
    return topicId;
  }
}
