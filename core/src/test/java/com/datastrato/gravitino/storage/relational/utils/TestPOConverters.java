/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestPOConverters {
  private static final LocalDateTime FIX_DATE_TIME = LocalDateTime.of(2024, 2, 6, 0, 0, 0);

  private static final Instant FIX_INSTANT = FIX_DATE_TIME.toInstant(ZoneOffset.UTC);

  @Test
  public void testFromMetalakePO() throws JsonProcessingException {
    MetalakePO metalakePO = createMetalakePO(1L, "test", "this is test");

    BaseMetalake expectedMetalake = createMetalake(1L, "test", "this is test");

    BaseMetalake convertedMetalake = POConverters.fromMetalakePO(metalakePO);

    // Assert
    assertEquals(expectedMetalake.id(), convertedMetalake.id());
    assertEquals(expectedMetalake.name(), convertedMetalake.name());
    assertEquals(expectedMetalake.comment(), convertedMetalake.comment());
    assertEquals(
        expectedMetalake.properties().get("key"), convertedMetalake.properties().get("key"));
    assertEquals(expectedMetalake.auditInfo().creator(), convertedMetalake.auditInfo().creator());
    assertEquals(expectedMetalake.getVersion(), convertedMetalake.getVersion());
  }

  @Test
  public void testFromCatalogPO() throws JsonProcessingException {
    CatalogPO catalogPO = createCatalogPO(1L, "test", 1L, "this is test");

    CatalogEntity expectedCatalog =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");

    CatalogEntity convertedCatalog =
        POConverters.fromCatalogPO(catalogPO, Namespace.ofCatalog("test_metalake"));

    // Assert
    assertEquals(expectedCatalog.id(), convertedCatalog.id());
    assertEquals(expectedCatalog.name(), convertedCatalog.name());
    assertEquals(expectedCatalog.getComment(), convertedCatalog.getComment());
    assertEquals(expectedCatalog.getType(), convertedCatalog.getType());
    assertEquals(expectedCatalog.getProvider(), convertedCatalog.getProvider());
    assertEquals(expectedCatalog.namespace(), convertedCatalog.namespace());
    assertEquals(
        expectedCatalog.getProperties().get("key"), convertedCatalog.getProperties().get("key"));
    assertEquals(expectedCatalog.auditInfo().creator(), convertedCatalog.auditInfo().creator());
  }

  @Test
  public void testFromMetalakePOs() throws JsonProcessingException {
    MetalakePO metalakePO1 = createMetalakePO(1L, "test", "this is test");
    MetalakePO metalakePO2 = createMetalakePO(2L, "test2", "this is test2");
    List<MetalakePO> metalakePOs = new ArrayList<>(Arrays.asList(metalakePO1, metalakePO2));
    List<BaseMetalake> convertedMetalakes = POConverters.fromMetalakePOs(metalakePOs);

    BaseMetalake expectedMetalake1 = createMetalake(1L, "test", "this is test");
    BaseMetalake expectedMetalake2 = createMetalake(2L, "test2", "this is test2");
    List<BaseMetalake> expectedMetalakes =
        new ArrayList<>(Arrays.asList(expectedMetalake1, expectedMetalake2));

    // Assert
    int index = 0;
    for (BaseMetalake metalake : convertedMetalakes) {
      assertEquals(expectedMetalakes.get(index).id(), metalake.id());
      assertEquals(expectedMetalakes.get(index).name(), metalake.name());
      assertEquals(expectedMetalakes.get(index).comment(), metalake.comment());
      assertEquals(
          expectedMetalakes.get(index).properties().get("key"), metalake.properties().get("key"));
      assertEquals(
          expectedMetalakes.get(index).auditInfo().creator(), metalake.auditInfo().creator());
      assertEquals(expectedMetalakes.get(index).getVersion(), metalake.getVersion());
      index++;
    }
  }

  @Test
  public void testFromCatalogPOs() throws JsonProcessingException {
    CatalogPO catalogPO1 = createCatalogPO(1L, "test", 1L, "this is test");
    CatalogPO catalogPO2 = createCatalogPO(2L, "test2", 1L, "this is test2");
    List<CatalogPO> catalogPOs = new ArrayList<>(Arrays.asList(catalogPO1, catalogPO2));
    List<CatalogEntity> convertedCatalogs =
        POConverters.fromCatalogPOs(catalogPOs, Namespace.ofCatalog("test_metalake"));

    CatalogEntity expectedCatalog1 =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");
    CatalogEntity expectedCatalog2 =
        createCatalog(2L, "test2", Namespace.ofCatalog("test_metalake"), "this is test2");
    List<CatalogEntity> expectedCatalogs =
        new ArrayList<>(Arrays.asList(expectedCatalog1, expectedCatalog2));

    // Assert
    int index = 0;
    for (CatalogEntity catalog : convertedCatalogs) {
      assertEquals(expectedCatalogs.get(index).id(), catalog.id());
      assertEquals(expectedCatalogs.get(index).name(), catalog.name());
      assertEquals(expectedCatalogs.get(index).getComment(), catalog.getComment());
      assertEquals(expectedCatalogs.get(index).getType(), catalog.getType());
      assertEquals(expectedCatalogs.get(index).getProvider(), catalog.getProvider());
      assertEquals(expectedCatalogs.get(index).namespace(), catalog.namespace());
      assertEquals(
          expectedCatalogs.get(index).getProperties().get("key"),
          catalog.getProperties().get("key"));
      assertEquals(
          expectedCatalogs.get(index).auditInfo().creator(), catalog.auditInfo().creator());
      index++;
    }
  }

  @Test
  public void testInitMetalakePOVersion() {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");
    MetalakePO initPO = POConverters.initializeMetalakePOWithVersion(metalake);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testInitCatalogPOVersion() {
    CatalogEntity catalog =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");
    CatalogPO initPO = POConverters.initializeCatalogPOWithVersion(catalog, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
  }

  @Test
  public void testUpdateMetalakePOVersion() {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");
    BaseMetalake updatedMetalake = createMetalake(1L, "test", "this is test2");
    MetalakePO initPO = POConverters.initializeMetalakePOWithVersion(metalake);
    MetalakePO updatePO = POConverters.updateMetalakePOWithVersion(initPO, updatedMetalake);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getMetalakeComment());
  }

  @Test
  public void testUpdateCatalogPOVersion() {
    CatalogEntity catalog =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");
    CatalogEntity updatedCatalog =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test2");
    CatalogPO initPO = POConverters.initializeCatalogPOWithVersion(catalog, 1L);
    CatalogPO updatePO = POConverters.updateCatalogPOWithVersion(initPO, updatedCatalog, 1L);
    assertEquals(1, initPO.getCurrentVersion());
    assertEquals(1, initPO.getLastVersion());
    assertEquals(0, initPO.getDeletedAt());
    assertEquals("this is test2", updatePO.getCatalogComment());
  }

  @Test
  public void testToMetalakePO() throws JsonProcessingException {
    BaseMetalake metalake = createMetalake(1L, "test", "this is test");

    MetalakePO expectedMetalakePO = createMetalakePO(1L, "test", "this is test");

    MetalakePO actualMetalakePO = POConverters.toMetalakePO(metalake);

    // Assert
    assertEquals(expectedMetalakePO.getMetalakeId(), actualMetalakePO.getMetalakeId());
    assertEquals(expectedMetalakePO.getMetalakeName(), actualMetalakePO.getMetalakeName());
    assertEquals(expectedMetalakePO.getMetalakeComment(), actualMetalakePO.getMetalakeComment());
    assertEquals(expectedMetalakePO.getProperties(), actualMetalakePO.getProperties());
    assertEquals(expectedMetalakePO.getAuditInfo(), actualMetalakePO.getAuditInfo());
    assertEquals(expectedMetalakePO.getSchemaVersion(), actualMetalakePO.getSchemaVersion());
  }

  @Test
  public void testToCatalogPO() throws JsonProcessingException {
    CatalogEntity catalog =
        createCatalog(1L, "test", Namespace.ofCatalog("test_metalake"), "this is test");

    CatalogPO expectedCatalogPO = createCatalogPO(1L, "test", 1L, "this is test");

    CatalogPO actualCatalogPO = POConverters.toCatalogPO(catalog, 1L);

    // Assert
    assertEquals(expectedCatalogPO.getCatalogId(), actualCatalogPO.getCatalogId());
    assertEquals(expectedCatalogPO.getMetalakeId(), actualCatalogPO.getMetalakeId());
    assertEquals(expectedCatalogPO.getCatalogName(), actualCatalogPO.getCatalogName());
    assertEquals(expectedCatalogPO.getType(), actualCatalogPO.getType());
    assertEquals(expectedCatalogPO.getProvider(), actualCatalogPO.getProvider());
    assertEquals(expectedCatalogPO.getCatalogComment(), actualCatalogPO.getCatalogComment());
    assertEquals(expectedCatalogPO.getProperties(), actualCatalogPO.getProperties());
    assertEquals(expectedCatalogPO.getAuditInfo(), actualCatalogPO.getAuditInfo());
  }

  private static BaseMetalake createMetalake(Long id, String name, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return new BaseMetalake.Builder()
        .withId(id)
        .withName(name)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static MetalakePO createMetalakePO(Long id, String name, String comment)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return new MetalakePO.Builder()
        .withMetalakeId(id)
        .withMetalakeName(name)
        .withMetalakeComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .withSchemaVersion(JsonUtils.anyFieldMapper().writeValueAsString(SchemaVersion.V_0_1))
        .build();
  }

  private static CatalogEntity createCatalog(
      Long id, String name, Namespace namespace, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static CatalogPO createCatalogPO(Long id, String name, Long metalakeId, String comment)
      throws JsonProcessingException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(FIX_INSTANT).build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    return new CatalogPO.Builder()
        .withCatalogId(id)
        .withCatalogName(name)
        .withMetalakeId(metalakeId)
        .withType(Catalog.Type.RELATIONAL.name())
        .withProvider("test")
        .withCatalogComment(comment)
        .withProperties(JsonUtils.anyFieldMapper().writeValueAsString(properties))
        .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(auditInfo))
        .build();
  }
}
