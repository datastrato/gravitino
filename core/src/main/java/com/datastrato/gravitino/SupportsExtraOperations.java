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

package com.datastrato.gravitino;

import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.TagEntity;
import java.io.IOException;

/**
 * An interface to support extra entity store operations, this interface should be mixed with {@link
 * EntityStore} to provide extra operations.
 *
 * <p>Any operations that can be done by the entity store should be added here.
 */
public interface SupportsExtraOperations {

  /**
   * List all the metadata objects that are associated with the given tag.
   *
   * @param tagIdent The identifier of the tag.
   * @return The list of metadata objects associated with the given tag.
   */
  MetadataObject[] listAssociatedMetadataObjectsForTag(NameIdentifier tagIdent) throws IOException;

  /**
   * List all the tags that are associated with the given metadata object.
   *
   * @param objectIdent The identifier of the metadata object.
   * @param objectType The type of the metadata object.
   * @return The list of tags associated with the given metadata object.
   * @throws NoSuchEntityException if the metadata object does not exist.
   */
  TagEntity[] listAssociatedTagsForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType)
      throws NoSuchEntityException, IOException;

  /**
   * Get the tag with the given identifier that is associated with the given metadata object.
   *
   * @param objectIdent The identifier of the metadata object.
   * @param objectType The type of the metadata object.
   * @param tagIdent The identifier of the tag.
   * @return The tag associated with the metadata object.
   * @throws NoSuchEntityException if the metadata object does not exist.
   */
  TagEntity getTagForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType, NameIdentifier tagIdent)
      throws NoSuchEntityException, IOException;

  /**
   * Associate the given tags with the given metadata object.
   *
   * @param objectIdent The identifier of the metadata object.
   * @param objectType The type of the metadata object.
   * @param tagsToAdd The name of tags to associate with the metadata object.
   * @param tagsToRemove the name of tags to remove from the metadata object.
   * @return The list of tags associated with the metadata object after the operation.
   * @throws NoSuchEntityException if the metadata object does not exist.
   */
  TagEntity[] associateTagsWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] tagsToAdd,
      NameIdentifier[] tagsToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException;
}
