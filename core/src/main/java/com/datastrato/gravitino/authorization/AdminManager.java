/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

public class AdminManager {

    private static final Logger LOG = LoggerFactory.getLogger(AdminManager.class);

    private final EntityStore store;
    private final IdGenerator idGenerator;

    public AdminManager(EntityStore store, IdGenerator idGenerator) {
        this.store = store;
        this.idGenerator = idGenerator;
    }
    public User addMetalakeAdmin(String user) {

        UserEntity userEntity =
                UserEntity.builder()
                        .withId(idGenerator.nextId())
                        .withName(user)
                        .withNamespace(
                                Namespace.of(
                                        BaseMetalake.SYSTEM_METALAKE_RESERVED_NAME,
                                        CatalogEntity.AUTHORIZATION_CATALOG_NAME,
                                        SchemaEntity.ADMIN_SCHEMA_NAME))
                        .withRoles(Lists.newArrayList())
                        .withAuditInfo(
                                AuditInfo.builder()
                                        .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                                        .withCreateTime(Instant.now())
                                        .build())
                        .build();
        try {
            store.put(userEntity, false /* overwritten */);
            return userEntity;
        } catch (EntityAlreadyExistsException e) {
            LOG.warn("User {} in the metalake admin already exists", user, e);
            throw new UserAlreadyExistsException(
                    "User %s in the metalake admin already exists", user);
        } catch (IOException ioe) {
            LOG.error(
                    "Adding user {} failed to the metalake admin due to storage issues",
                    user,
                    ioe);
            throw new RuntimeException(ioe);
        }
    }

    public boolean removeMetalakeAdmin(String user) {
        try {
            return store.delete(ofMetalakeAdmin(user), Entity.EntityType.USER);
        } catch (IOException ioe) {
            LOG.error(
                    "Removing user {} from the metalake admin {} failed due to storage issues", user, ioe);
            throw new RuntimeException(ioe);
        }
    }

    public boolean isServiceAdmin(String user) {
        String admin = GravitinoEnv.getInstance().config().get(Configs.SERVICE_ADMIN);
        return admin.equals(user);
    }

    public boolean isMetalakeAdmin(String user) {
        try {
            return store.exists(ofMetalakeAdmin(user), Entity.EntityType.USER);
        } catch (IOException ioe) {
            LOG.error(
                "Fail to check {} from the metalake admin {} due to storage issues", user, ioe);
            throw new RuntimeException(ioe);
        }
    }

    private NameIdentifier ofMetalakeAdmin(String user) {
        return NameIdentifier.of(BaseMetalake.SYSTEM_METALAKE_RESERVED_NAME, CatalogEntity.AUTHORIZATION_CATALOG_NAME, SchemaEntity.ADMIN_SCHEMA_NAME, user);
    }
}
