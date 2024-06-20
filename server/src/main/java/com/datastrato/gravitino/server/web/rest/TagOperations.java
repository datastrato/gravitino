/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.MetadataObjects;
import com.datastrato.gravitino.dto.requests.TagCreateRequest;
import com.datastrato.gravitino.dto.requests.TagUpdateRequest;
import com.datastrato.gravitino.dto.requests.TagUpdatesRequest;
import com.datastrato.gravitino.dto.requests.TagsAssociateRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.NameListResponse;
import com.datastrato.gravitino.dto.responses.TagListResponse;
import com.datastrato.gravitino.dto.tag.TagDTO;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.NoSuchTagException;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import com.datastrato.gravitino.tag.Tag;
import com.datastrato.gravitino.tag.TagChange;
import com.datastrato.gravitino.tag.TagManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@Path("metalakes/{metalake}/tags")
public class TagOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TagOperations.class);

  private final TagManager tagManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public TagOperations(TagManager tagManager) {
    this.tagManager = tagManager;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-tags", absolute = true)
  public Response listTags(
      @PathParam("metalake") String metalake,
      @QueryParam("details") @DefaultValue("false") boolean verbose,
      @QueryParam("extended") @DefaultValue("false") boolean extended) {
    LOG.info(
        "Received list tag {} with extended {} request for metalake: {}",
        verbose? "infos" : "names",
        extended,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (verbose) {
              Tag[] tags = tagManager.listTagsInfo(metalake, extended);
              TagDTO[] tagDTOs;
              if (ArrayUtils.isEmpty(tags)) {
                tagDTOs = new TagDTO[0];
              } else {
                tagDTOs = Arrays.stream(tags)
                    .map(t -> DTOConverters.toDTO(t, Optional.empty()))
                    .toArray(TagDTO[]::new);
              }

              LOG.info(
                  "List {} tags info with extended {} under metalake: {}",
                  tags.length,
                  extended,
                  metalake);
              return Utils.ok(new TagListResponse(tagDTOs));

            } else {
              String[] tagNames = tagManager.listTags(metalake);
              tagNames = tagNames == null ? new String[0] : tagNames;

              LOG.info("List {} tags under metalake: {}", tagNames.length, metalake);
              return Utils.ok(new NameListResponse(tagNames));
            }
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.LIST, "", metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-tag", absolute = true)
  public Response createTag(
      @PathParam("metalake") String metalake, TagCreateRequest request) {
    LOG.info("Received create tag request under metalake: {}", metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            Tag tag = tagManager.createTag(
                metalake, request.getName(), request.getComment(), request.getProperties());

            LOG.info("Created tag: {} under metalake: {}", tag.name(), metalake);
            return Utils.ok(DTOConverters.toDTO(tag, Optional.empty()));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @GET
  @Path("{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-tag", absolute = true)
  public Response getTag(
      @PathParam("metalake") String metalake,
      @PathParam("tag") String name,
      @QueryParam("extended") @DefaultValue("false") boolean extended) {
    LOG.info("Received get tag request for tag: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Tag tag = tagManager.getTag(metalake, name, extended);
            LOG.info("Get tag: {} under metalake: {}", name, metalake);
            return Utils.ok(DTOConverters.toDTO(tag, Optional.empty()));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.GET, name, metalake, e);
    }
  }

  @POST
  @Path("{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-tag", absolute = true)
  public Response alterTag(
      @PathParam("metalake") String metalake,
      @PathParam("tag") String name,
      TagUpdatesRequest request) {
    LOG.info("Received alter tag request for tag: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();

            TagChange[] changes =
                request.getUpdates().stream()
                .map(TagUpdateRequest::tagChange)
                .toArray(TagChange[]::new);
            Tag tag = tagManager.alterTag(metalake, name, changes);

            LOG.info("Altered tag: {} under metalake: {}", name, metalake);
            return Utils.ok(DTOConverters.toDTO(tag, Optional.empty()));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.ALTER, name, metalake, e);
    }
  }

  @DELETE
  @Path("{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-tag", absolute = true)
  public Response deleteTag(@PathParam("metalake") String metalake, @PathParam("tag") String name) {
    LOG.info("Received delete tag request for tag: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = tagManager.deleteTag(metalake, name);
            if (!deleted) {
              LOG.warn("Failed to delete tag {} under metalake {}", name, metalake);
            } else {
              LOG.info("Deleted tag: {} under metalake: {}", name, metalake);
            }

            return Utils.ok(new DropResponse(deleted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.DELETE, name, metalake, e);
    }
  }

  @GET
  @Path("{type}/{fullName}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-object-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-tags", absolute = true)
  public Response listTagsForObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    LOG.info(
        "Received list tag {} request for object type: {}, full name: {} under metalake: {}",
        verbose? "infos" : "names",
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object = MetadataObjects.parse(fullName, toType(type));

            if (verbose) {
              List<TagDTO> tags = Lists.newArrayList();
              Tag[] nonInheritedTags = tagManager.listTagsInfoForMetadataObject(metalake, object);
              if (ArrayUtils.isNotEmpty(nonInheritedTags)) {
                Collections.addAll(
                    tags,
                    Arrays.stream(nonInheritedTags)
                        .map(t -> DTOConverters.toDTO(t, Optional.of(false)))
                        .toArray(TagDTO[]::new));
              }

              MetadataObject parentObject = MetadataObjects.parent(object);
              while (parentObject != null) {
                Tag[] heritageTags =
                    tagManager.listTagsInfoForMetadataObject(metalake, parentObject);
                if (ArrayUtils.isNotEmpty(heritageTags)) {
                  Collections.addAll(
                      tags,
                      Arrays.stream(heritageTags)
                          .map(t -> DTOConverters.toDTO(t, Optional.of(true)))
                          .toArray(TagDTO[]::new));
                }
                parentObject = MetadataObjects.parent(parentObject);
              }

              LOG.info(
                  "List {} tags info for object type: {}, full name: {} under metalake: {}",
                  tags.size(),
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new TagListResponse(tags.toArray(new TagDTO[0])));

            } else {
              List<String> tagNames = Lists.newArrayList();
              String[] nonInheritedTagNames =
                  tagManager.listTagsForMetadataObject(metalake, object);
              if (ArrayUtils.isNotEmpty(nonInheritedTagNames)) {
                Collections.addAll(tagNames, nonInheritedTagNames);
              }

              MetadataObject parentObject = MetadataObjects.parent(object);
              while (parentObject != null) {
                String[] heritageTagNames =
                    tagManager.listTagsForMetadataObject(metalake, parentObject);
                if (ArrayUtils.isNotEmpty(heritageTagNames)) {
                  Collections.addAll(tagNames, heritageTagNames);
                }
                parentObject = MetadataObjects.parent(parentObject);
              }

              LOG.info(
                  "List {} tags for object type: {}, full name: {} under metalake: {}",
                  tagNames.stream(),
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new NameListResponse(tagNames.toArray(new String[0])));
            }
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.LIST, "", fullName, e);
    }
  }

  @GET
  @Path("{type}/{fullName}/{tag}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-object-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-object-tag", absolute = true)
  public Response getTagForObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @PathParam("tag") String tagName) {
    LOG.info(
        "Received get tag {} request for object type: {}, full name: {} under metalake: {}",
        tagName,
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object = MetadataObjects.parse(fullName, toType(type));
            Optional<Tag> tag = getTagForObject(metalake, object, tagName);
            Optional<TagDTO> tagDTO =
                tag.map(t -> DTOConverters.toDTO(t, Optional.of(false)));

            MetadataObject parentObject = MetadataObjects.parent(object);
            while (!tag.isPresent() && parentObject != null) {
              tag = getTagForObject(metalake, parentObject, tagName);
              tagDTO = tag.map(t -> DTOConverters.toDTO(t, Optional.of(true)));
              parentObject = MetadataObjects.parent(parentObject);
            }

            if (!tagDTO.isPresent()) {
              LOG.warn("Tag {} not found for object type: {}, full name: {} under metalake: {}",
                  tagName, type, fullName, metalake);
              return Utils.notFound(
                  NoSuchTagException.class.getSimpleName(),
                  "Tag not found: " + tagName + " for object type: " + type +
                      ", full name: " + fullName + " under metalake: " + metalake);
            } else {
              LOG.info(
                  "Get tag: {} for object type: {}, full name: {} under metalake: {}",
                  tagName,
                  type,
                  fullName,
                  metalake);
              return Utils.ok(tagDTO.get());
            }
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.GET, tagName, fullName, e);
    }
  }

  @POST
  @Path("{type}/{fullName}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "associate-object-tags." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "associate-object-tags", absolute = true)
  public Response associateTagsForObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      TagsAssociateRequest request) {
    LOG.info(
        "Received associate tags request for object type: {}, full name: {} under metalake: {}",
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObject object = MetadataObjects.parse(fullName, toType(type));
            String[] tagNames = tagManager.associateTagsForMetadataObject(
                metalake, object, request.getTagsToAdd(), request.getTagsToRemove());

            LOG.info(
                "Associated tags: {} for object type: {}, full name: {} under metalake: {}",
                Arrays.toString(tagNames),
                type,
                fullName,
                metalake);
            return Utils.ok(new NameListResponse(tagNames));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTagException(OperationType.ASSOCIATE, "", fullName, e);
    }
  }

  private MetadataObject.Type toType(String type) {
    try {
      return MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid type: " + type);
    }
  }

  private Optional<Tag> getTagForObject(String metalake, MetadataObject object, String tagName) {
    try {
      return Optional.of(tagManager.getTagForMetadataObject(metalake, object, tagName));
    } catch (NoSuchTagException e) {
      LOG.info("Tag {} not found for object: {}", tagName, object);
      return Optional.empty();
    }
  }









}
