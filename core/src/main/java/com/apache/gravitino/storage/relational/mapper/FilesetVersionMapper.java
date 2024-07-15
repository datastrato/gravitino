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

package com.apache.gravitino.storage.relational.mapper;

import com.apache.gravitino.storage.relational.po.FilesetMaxVersionPO;
import com.apache.gravitino.storage.relational.po.FilesetVersionPO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for fileset version info operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface FilesetVersionMapper {
  String VERSION_TABLE_NAME = "fileset_version_info";

  @Insert(
      "INSERT INTO "
          + VERSION_TABLE_NAME
          + "(metalake_id, catalog_id, schema_id, fileset_id,"
          + " version, fileset_comment, properties, storage_location,"
          + " deleted_at)"
          + " VALUES("
          + " #{filesetVersion.metalakeId},"
          + " #{filesetVersion.catalogId},"
          + " #{filesetVersion.schemaId},"
          + " #{filesetVersion.filesetId},"
          + " #{filesetVersion.version},"
          + " #{filesetVersion.filesetComment},"
          + " #{filesetVersion.properties},"
          + " #{filesetVersion.storageLocation},"
          + " #{filesetVersion.deletedAt}"
          + " )")
  void insertFilesetVersion(@Param("filesetVersion") FilesetVersionPO filesetVersionPO);

  @Insert(
      "INSERT INTO "
          + VERSION_TABLE_NAME
          + "(metalake_id, catalog_id, schema_id, fileset_id,"
          + " version, fileset_comment, properties, storage_location,"
          + " deleted_at)"
          + " VALUES("
          + " #{filesetVersion.metalakeId},"
          + " #{filesetVersion.catalogId},"
          + " #{filesetVersion.schemaId},"
          + " #{filesetVersion.filesetId},"
          + " #{filesetVersion.version},"
          + " #{filesetVersion.filesetComment},"
          + " #{filesetVersion.properties},"
          + " #{filesetVersion.storageLocation},"
          + " #{filesetVersion.deletedAt}"
          + " )"
          + " ON DUPLICATE KEY UPDATE"
          + " metalake_id = #{filesetVersion.metalakeId},"
          + " catalog_id = #{filesetVersion.catalogId},"
          + " schema_id = #{filesetVersion.schemaId},"
          + " fileset_id = #{filesetVersion.filesetId},"
          + " version = #{filesetVersion.version},"
          + " fileset_comment = #{filesetVersion.filesetComment},"
          + " properties = #{filesetVersion.properties},"
          + " storage_location = #{filesetVersion.storageLocation},"
          + " deleted_at = #{filesetVersion.deletedAt}")
  void insertFilesetVersionOnDuplicateKeyUpdate(
      @Param("filesetVersion") FilesetVersionPO filesetVersionPO);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByCatalogId(@Param("catalogId") Long catalogId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsBySchemaId(@Param("schemaId") Long schemaId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE fileset_id = #{filesetId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByFilesetId(@Param("filesetId") Long filesetId);

  @Delete(
      "DELETE FROM "
          + VERSION_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteFilesetVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @Select(
      "SELECT fileset_id as filesetId,"
          + " Max(version) as version"
          + " FROM "
          + VERSION_TABLE_NAME
          + " WHERE version > #{versionRetentionCount} AND deleted_at = 0"
          + " GROUP BY fileset_id")
  List<FilesetMaxVersionPO> selectFilesetVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE fileset_id = #{filesetId} AND version <= #{versionRetentionLine} AND deleted_at = 0 LIMIT #{limit}")
  Integer softDeleteFilesetVersionsByRetentionLine(
      @Param("filesetId") Long filesetId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit);
}
