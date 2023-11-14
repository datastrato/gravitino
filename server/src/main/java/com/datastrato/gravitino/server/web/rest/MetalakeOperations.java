/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.requests.MetalakeCreateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdateRequest;
import com.datastrato.gravitino.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.MetalakeManager;
import com.datastrato.gravitino.server.web.Utils;
import com.datastrato.gravitino.utils.Constants;
import java.util.Arrays;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MetalakeOperations {

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeOperations.class);

  private final MetalakeManager metalakeManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetalakeOperations(MetalakeManager metaManager) {
    this.metalakeManager = metaManager;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  public Response listMetalakes() {
    try {
      BaseMetalake[] metalakes = metalakeManager.listMetalakes();
      MetalakeDTO[] metalakeDTOS =
          Arrays.stream(metalakes).map(DTOConverters::toDTO).toArray(MetalakeDTO[]::new);
      return Utils.ok(new MetalakeListResponse(metalakeDTOS));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(
          OperationType.LIST, Namespace.empty().toString(), e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  public Response createMetalake(MetalakeCreateRequest request) {
    try {

      request.validate();
      NameIdentifier ident = NameIdentifier.ofMetalake(request.getName());
      BaseMetalake metalake =
          metalakeManager.createMetalake(ident, request.getComment(), request.getProperties());
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.CREATE, request.getName(), e);
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response loadMetalake(@PathParam("name") String metalakeName) {
    try {
      NameIdentifier identifier = NameIdentifier.ofMetalake(metalakeName);
      BaseMetalake metalake = metalakeManager.loadMetalake(identifier);
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.LOAD, metalakeName, e);
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response alterMetalake(
      @PathParam("name") String metalakeName, MetalakeUpdatesRequest updatesRequest) {
    try {
      updatesRequest.validate();
      NameIdentifier identifier = NameIdentifier.ofMetalake(metalakeName);
      MetalakeChange[] changes =
          updatesRequest.getUpdates().stream()
              .map(MetalakeUpdateRequest::metalakeChange)
              .toArray(MetalakeChange[]::new);

      BaseMetalake updatedMetalake = metalakeManager.alterMetalake(identifier, changes);
      return Utils.ok(new MetalakeResponse(DTOConverters.toDTO(updatedMetalake)));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.ALTER, metalakeName, e);
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  public Response dropMetalake(
      @HeaderParam(Constants.HTTP_HEADER_NAME) String authData,
      @PathParam("name") String metalakeName) {
    try {
      NameIdentifier identifier = NameIdentifier.ofMetalake(metalakeName);
      boolean dropped = metalakeManager.dropMetalake(identifier);
      if (!dropped) {
        LOG.warn("Failed to drop metalake by name {}", metalakeName);
      }

      return Utils.ok(new DropResponse(dropped));

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.DROP, metalakeName, e);
    }
  }
}
