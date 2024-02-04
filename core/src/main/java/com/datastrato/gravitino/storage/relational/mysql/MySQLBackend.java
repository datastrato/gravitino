/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.mysql;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.storage.relational.RelationalBackend;
import com.datastrato.gravitino.storage.relation.mysql.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relation.mysql.orm.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relation.mysql.orm.SqlSessions;
import com.datastrato.gravitino.storage.relation.mysql.po.MetalakePO;
import com.datastrato.gravitino.storage.relation.mysql.utils.POConverters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ibatis.session.SqlSession;

/**
 * {@link MySQLBackend} is a MySQL implementation of RelationalBackend interface. If we want to use
 * another relational implementation, We can just implement {@link RelationalBackend} interface and
 * use it in the Gravitino.
 */
public class MySQLBackend implements RelationalBackend {

  /** Initialize the MySQL backend instance. */
  @Override
  public void initialize(Config config) {
    SqlSessionFactoryHelper.getInstance().init(config);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      switch (entityType) {
        case METALAKE:
          List<MetalakePO> metalakePOS =
              ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                  .listMetalakePOs();
          return (List<E>)
              metalakePOS.stream()
                  .map(
                      metalakePO -> {
                        try {
                          return POConverters.fromMetalakePO(metalakePO);
                        } catch (JsonProcessingException e) {
                          throw new RuntimeException(e);
                        }
                      })
                  .collect(Collectors.toList());
        case CATALOG:
        case SCHEMA:
        case TABLE:
        case FILESET:
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported entity type: %s for list operation", entityType));
      }
    } finally {
      SqlSessions.closeSqlSession();
    }
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      switch (entityType) {
        case METALAKE:
          MetalakePO metalakePO =
              ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                  .selectMetalakeMetaByName(ident.name());
          return metalakePO != null;
        case CATALOG:
        case SCHEMA:
        case TABLE:
        case FILESET:
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported entity type: %s for exists operation", entityType));
      }
    } finally {
      SqlSessions.closeSqlSession();
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        if (e instanceof BaseMetalake) {
          MetalakePO metalakePO = POConverters.toMetalakePO((BaseMetalake) e);
          if (overwritten) {
            ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                .insertMetalakeMetaWithUpdate(metalakePO);
          } else {
            ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                .insertMetalakeMeta(metalakePO);
          }
          SqlSessions.commitAndCloseSqlSession();
        } else {
          throw new IllegalArgumentException(
              String.format("Unsupported entity type: %s for put operation", e.getClass()));
        }
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws NoSuchEntityException, AlreadyExistsException {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        switch (entityType) {
          case METALAKE:
            MetalakePO oldMetalakePO =
                ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                    .selectMetalakeMetaByName(ident.name());
            BaseMetalake oldMetalakeEntity = POConverters.fromMetalakePO(oldMetalakePO);
            BaseMetalake newMetalakeEntity = (BaseMetalake) updater.apply((E) oldMetalakeEntity);
            Preconditions.checkArgument(
                Objects.equals(oldMetalakeEntity.id(), newMetalakeEntity.id()),
                String.format(
                    "The updated metalake entity id: %s is not same with the metalake entity id before: %s",
                    newMetalakeEntity.id(), oldMetalakeEntity.id()));
            ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                .updateMetalakeMeta(POConverters.toMetalakePO(newMetalakeEntity), oldMetalakePO);
            SqlSessions.commitAndCloseSqlSession();
            return (E) newMetalakeEntity;
          case CATALOG:
          case SCHEMA:
          case TABLE:
          case FILESET:
          default:
            throw new IllegalArgumentException(
                String.format("Unsupported entity type: %s for update operation", entityType));
        }
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType)
      throws NoSuchEntityException, IOException {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      switch (entityType) {
        case METALAKE:
          MetalakePO metalakePO =
              ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                  .selectMetalakeMetaByName(ident.name());
          return (E) POConverters.fromMetalakePO(metalakePO);
        case CATALOG:
        case SCHEMA:
        case TABLE:
        case FILESET:
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported entity type: %s for get operation", entityType));
      }
    } finally {
      SqlSessions.closeSqlSession();
    }
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        switch (entityType) {
          case METALAKE:
            Long metalakeId =
                ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                    .selectMetalakeIdMetaByName(ident.name());
            if (metalakeId != null) {
              // delete metalake
              ((MetalakeMetaMapper) SqlSessions.getMapper(MetalakeMetaMapper.class))
                  .deleteMetalakeMetaById(metalakeId);
              if (cascade) {
                // TODO We will cascade delete the metadata of sub-resources under metalake
              }
              SqlSessions.commitAndCloseSqlSession();
            }
            return true;
          case CATALOG:
          case SCHEMA:
          case TABLE:
          case FILESET:
          default:
            throw new IllegalArgumentException(
                String.format("Unsupported entity type: %s for delete operation", entityType));
        }
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw new RuntimeException(t);
      }
    }
  }

  @Override
  public void close() throws IOException {}
}
