/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.dto.authorization.UserDTO;
import com.datastrato.gravitino.dto.requests.UserAddRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.rest.RESTUtils;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestUserOperations extends JerseyTest {

  private final AccessControlManager manager = mock(AccessControlManager.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  public static void setup() {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    GravitinoEnv.getInstance().setLockManager(new LockManager(config));
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(UserOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(manager).to(AccessControlManager.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testAddUser() {
    UserAddRequest req = new UserAddRequest("user1");
    User user = buildUser("user1");

    when(manager.addUser(any(), any())).thenReturn(user);

    Response resp =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    UserResponse userResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());

    UserDTO userDTO = userResponse.getUser();
    Assertions.assertEquals("user1", userDTO.name());
    Assertions.assertNotNull(userDTO.roles());
    Assertions.assertTrue(userDTO.roles().isEmpty());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).addUser(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test throw UserAlreadyExistsException
    doThrow(new UserAlreadyExistsException("mock error")).when(manager).addUser(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse1.getCode());
    Assertions.assertEquals(
        UserAlreadyExistsException.class.getSimpleName(), errorResponse1.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).addUser(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/users")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testGetUser() {

    User user = buildUser("user1");

    when(manager.getUser(any(), any())).thenReturn(user);

    Response resp =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    UserResponse userResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());
    UserDTO userDTO = userResponse.getUser();
    Assertions.assertEquals("user1", userDTO.name());
    Assertions.assertNotNull(userDTO.roles());
    Assertions.assertTrue(userDTO.roles().isEmpty());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).getUser(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error")).when(manager).getUser(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse1.getType());

    // Test throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).getUser(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  private User buildUser(String user) {
    return UserEntity.builder()
        .withId(1L)
        .withName(user)
        .withRoles(Collections.emptyList())
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  @Test
  public void testDropUser() {
    when(manager.removeUser(any(), any())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test when failed to drop user
    when(manager.removeUser(any(), any())).thenReturn(false);
    Response resp2 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    DropResponse dropResponse2 = resp2.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse2.getCode());
    Assertions.assertFalse(dropResponse2.dropped());

    doThrow(new RuntimeException("mock error")).when(manager).removeUser(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/users/user1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }
}
