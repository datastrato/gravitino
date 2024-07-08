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
package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.SupportsExtraOperations;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchTagException;
import com.datastrato.gravitino.exceptions.NotFoundException;
import com.datastrato.gravitino.exceptions.TagAlreadyAssociatedException;
import com.datastrato.gravitino.exceptions.TagAlreadyExistsException;
import com.datastrato.gravitino.lock.LockType;
import com.datastrato.gravitino.lock.TreeLockUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.TagEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.kv.KvEntityStore;
import com.datastrato.gravitino.utils.MetadataObjectUtil;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagManager {

  private static final Logger LOG = LoggerFactory.getLogger(TagManager.class);

  private final IdGenerator idGenerator;

  private final EntityStore entityStore;

  private final SupportsExtraOperations supportsExtraOperations;

  public TagManager(IdGenerator idGenerator, EntityStore entityStore) {
    if (entityStore instanceof KvEntityStore) {
      String errorMsg =
          "TagManager cannot run with kv entity store, please configure the entity "
              + "store to use relational entity store and restart the Gravitino server";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    if (!(entityStore instanceof SupportsExtraOperations)) {
      String errorMsg =
          "TagManager cannot run with entity store that does not support extra operations, "
              + "please configure the entity store to use relational entity store and restart the Gravitino server";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    this.supportsExtraOperations = entityStore.extraOperations();

    this.idGenerator = idGenerator;
    this.entityStore = entityStore;
  }

  public String[] listTags(String metalake) {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.READ,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore
                .list(ofTagNamespace(metalake), TagEntity.class, Entity.EntityType.TAG).stream()
                .map(TagEntity::name)
                .toArray(String[]::new);
          } catch (IOException ioe) {
            LOG.error("Failed to list tags under metalake {}", metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public Tag createTag(String metalake, String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException {
    Map<String, String> tagProperties = properties == null ? Collections.emptyMap() : properties;

    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          TagEntity tagEntity =
              TagEntity.builder()
                  .withId(idGenerator.nextId())
                  .withName(name)
                  .withNamespace(ofTagNamespace(metalake))
                  .withComment(comment)
                  .withProperties(tagProperties)
                  .withAuditInfo(
                      AuditInfo.builder()
                          .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                          .withCreateTime(Instant.now())
                          .build())
                  .build();

          try {
            entityStore.put(tagEntity, false /* overwritten */);
            return tagEntity;
          } catch (EntityAlreadyExistsException e) {
            throw new TagAlreadyExistsException(
                "Tag with name %s under metalake %s already exists", name, metalake);
          } catch (IOException ioe) {
            LOG.error("Failed to create tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    return TreeLockUtils.doWithTreeLock(
        ofTagIdent(metalake, name),
        LockType.READ,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore.get(
                ofTagIdent(metalake, name), Entity.EntityType.TAG, TagEntity.class);
          } catch (NoSuchEntityException e) {
            throw new NoSuchTagException(
                "Tag with name %s under metalake %s does not exist", name, metalake);
          } catch (IOException ioe) {
            LOG.error("Failed to get tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public Tag alterTag(String metalake, String name, TagChange... changes)
      throws NoSuchTagException, IllegalArgumentException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore.update(
                ofTagIdent(metalake, name),
                TagEntity.class,
                Entity.EntityType.TAG,
                tagEntity -> updateTagEntity(tagEntity, changes));
          } catch (NoSuchEntityException e) {
            throw new NoSuchTagException(
                "Tag with name %s under metalake %s does not exist", name, metalake);
          } catch (EntityAlreadyExistsException e) {
            throw new RuntimeException(
                "Tag with name " + name + " under metalake " + metalake + " already exists");
          } catch (IOException ioe) {
            LOG.error("Failed to alter tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public boolean deleteTag(String metalake, String name) {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ofTagNamespace(metalake).levels()),
        LockType.WRITE,
        () -> {
          checkMetalakeExists(metalake, entityStore);

          try {
            return entityStore.delete(ofTagIdent(metalake, name), Entity.EntityType.TAG);
          } catch (IOException ioe) {
            LOG.error("Failed to delete tag {} under metalake {}", name, metalake, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name)
      throws NoSuchTagException {
    NameIdentifier tagId = ofTagIdent(metalake, name);
    return TreeLockUtils.doWithTreeLock(
        tagId,
        LockType.READ,
        () -> {
          try {
            if (!entityStore.exists(tagId, Entity.EntityType.TAG)) {
              throw new NoSuchTagException(
                  "Tag with name %s under metalake %s does not exist", name, metalake);
            }

            return supportsExtraOperations.listAssociatedMetadataObjectsForTag(tagId);
          } catch (IOException e) {
            LOG.error("Failed to list metadata objects for tag {}", name, e);
            throw new RuntimeException(e);
          }
        });
  }

  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject)
      throws NotFoundException {
    return Arrays.stream(listTagsInfoForMetadataObject(metalake, metadataObject))
        .map(Tag::name)
        .toArray(String[]::new);
  }

  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject)
      throws NotFoundException {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);

    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () -> {
          try {
            return supportsExtraOperations.listAssociatedTagsForMetadataObject(
                entityIdent, entityType);
          } catch (NoSuchEntityException e) {
            throw new NotFoundException(
                e, "Failed to list tags for metadata object %s due to not found", metadataObject);
          } catch (IOException e) {
            LOG.error("Failed to list tags for metadata object {}", metadataObject, e);
            throw new RuntimeException(e);
          }
        });
  }

  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name)
      throws NotFoundException {
    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);
    NameIdentifier tagIdent = ofTagIdent(metalake, name);

    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () -> {
          try {
            return supportsExtraOperations.getTagForMetadataObject(
                entityIdent, entityType, tagIdent);
          } catch (NoSuchEntityException e) {
            if (e.getMessage().contains("No such tag entity")) {
              throw new NoSuchTagException(
                  e, "Tag %s does not exist for metadata object %s", name, metadataObject);
            } else {
              throw new NotFoundException(
                  e, "Failed to get tag for metadata object %s due to not found", metadataObject);
            }
          } catch (IOException e) {
            LOG.error("Failed to get tag for metadata object {}", metadataObject, e);
            throw new RuntimeException(e);
          }
        });
  }

  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove)
      throws NotFoundException, TagAlreadyAssociatedException {
    Preconditions.checkArgument(
        !metadataObject.type().equals(MetadataObject.Type.METALAKE)
            && !metadataObject.type().equals(MetadataObject.Type.COLUMN),
        "Cannot associate tags for unsupported metadata object type %s",
        metadataObject.type());

    NameIdentifier entityIdent = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataObject);

    Set<String> tagsToAddSet = tagsToAdd == null ? Sets.newHashSet() : Sets.newHashSet(tagsToAdd);
    if (tagsToRemove != null) {
      for (String tag : tagsToRemove) {
        tagsToAddSet.remove(tag);
      }
    }

    NameIdentifier[] tagsToAddIdent =
        tagsToAddSet.stream().map(tag -> ofTagIdent(metalake, tag)).toArray(NameIdentifier[]::new);
    NameIdentifier[] tagsToRemoveIdent =
        tagsToRemove == null
            ? new NameIdentifier[0]
            : Sets.newHashSet(tagsToRemove).stream()
                .map(tag -> ofTagIdent(metalake, tag))
                .toArray(NameIdentifier[]::new);

    // TODO. We need to add a write lock to Tag's namespace to avoid tag alteration and deletion
    //  during the association operation.
    return TreeLockUtils.doWithTreeLock(
        entityIdent,
        LockType.READ,
        () -> {
          try {
            return Arrays.stream(
                    supportsExtraOperations.associateTagsWithMetadataObject(
                        entityIdent, entityType, tagsToAddIdent, tagsToRemoveIdent))
                .map(Tag::name)
                .toArray(String[]::new);
          } catch (NoSuchEntityException e) {
            throw new NotFoundException(
                e,
                "Failed to associate tags for metadata object %s due to not found",
                metadataObject);
          } catch (EntityAlreadyExistsException e) {
            throw new TagAlreadyAssociatedException(
                e,
                "Failed to associate tags for metadata object due to some tags %s already "
                    + "associated to the metadata object %s",
                Arrays.toString(tagsToAdd),
                metadataObject);
          } catch (IOException e) {
            LOG.error("Failed to associate tags for metadata object {}", metadataObject, e);
            throw new RuntimeException(e);
          }
        });
  }

  private static void checkMetalakeExists(String metalake, EntityStore entityStore) {
    try {
      NameIdentifier metalakeIdent = NameIdentifier.of(metalake);
      if (!entityStore.exists(metalakeIdent, Entity.EntityType.METALAKE)) {
        LOG.warn("Metalake {} does not exist", metalakeIdent);
        throw new NoSuchMetalakeException("Metalake %s does not exist", metalakeIdent);
      }
    } catch (IOException ioe) {
      LOG.error("Failed to check if metalake exists", ioe);
      throw new RuntimeException(ioe);
    }
  }

  @VisibleForTesting
  public static Namespace ofTagNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.TAG_SCHEMA_NAME);
  }

  public static NameIdentifier ofTagIdent(String metalake, String tagName) {
    return NameIdentifier.of(ofTagNamespace(metalake), tagName);
  }

  private TagEntity updateTagEntity(TagEntity tagEntity, TagChange... changes) {
    Map<String, String> props =
        tagEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(tagEntity.properties());
    String newName = tagEntity.name();
    String newComment = tagEntity.comment();

    for (TagChange change : changes) {
      if (change instanceof TagChange.RenameTag) {
        newName = ((TagChange.RenameTag) change).getNewName();
      } else if (change instanceof TagChange.UpdateTagComment) {
        newComment = ((TagChange.UpdateTagComment) change).getNewComment();
      } else if (change instanceof TagChange.SetProperty) {
        TagChange.SetProperty setProperty = (TagChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof TagChange.RemoveProperty) {
        TagChange.RemoveProperty removeProperty = (TagChange.RemoveProperty) change;
        props.remove(removeProperty.getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported tag change: " + change);
      }
    }

    return TagEntity.builder()
        .withId(tagEntity.id())
        .withName(newName)
        .withNamespace(tagEntity.namespace())
        .withComment(newComment)
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(tagEntity.auditInfo().creator())
                .withCreateTime(tagEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }
}
