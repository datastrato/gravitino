/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_OPERATION_FAILED;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.trino.connector.GravitinoConfig;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the following main functions:
 *
 * <pre>
 * 1. Load catalogs from the Gravitino server and create
 * catalog contexts.
 * 2. Manage all catalog context instances, which primarily handle communication
 * with Trino through Gravitino connectors and inner connectors related to the engine.
 * </pre>
 */
public class CatalogConnectorManager {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorManager.class);

  private static final int CATALOG_LOAD_FREQUENCY_SECOND = 10;
  private static final int NUMBER_EXECUTOR_THREAD = 1;
  private static final int LOAD_METALAKE_TIMEOUT = 30;

  private final ScheduledExecutorService executorService;
  private final CatalogRegister catalogRegister;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  private final Set<String> usedMetalakes = new HashSet<>();
  private final Map<String, GravitinoMetalake> metalakes = new ConcurrentHashMap<>();

  private GravitinoAdminClient gravitinoClient;
  private GravitinoConfig config;

  public CatalogConnectorManager(
      CatalogRegister catalogRegister, CatalogConnectorFactory catalogFactory) {
    this.catalogRegister = catalogRegister;
    this.catalogConnectorFactory = catalogFactory;
    this.executorService = createScheduledThreadPoolExecutor();
  }

  private static ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor() {
    return new ScheduledThreadPoolExecutor(
        NUMBER_EXECUTOR_THREAD,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("gravitino-connector-schedule-%d")
            .setUncaughtExceptionHandler(
                (thread, throwable) ->
                    LOG.warn("{} uncaught exception:", thread.getName(), throwable))
            .build());
  }

  public void config(GravitinoConfig config, GravitinoAdminClient client) {
    this.config = Preconditions.checkNotNull(config, "config is not null");
    if (client == null) {
      this.gravitinoClient = GravitinoAdminClient.builder(config.getURI()).build();
    } else {
      this.gravitinoClient = client;
    }
  }

  public void start(ConnectorContext context) throws Exception {
    catalogRegister.init(context, config);
    if (catalogRegister.isCoordinator()) {
      executorService.scheduleWithFixedDelay(
          this::loadMetalake,
          CATALOG_LOAD_FREQUENCY_SECOND,
          CATALOG_LOAD_FREQUENCY_SECOND,
          TimeUnit.SECONDS);
    }

    LOG.info("Gravitino CatalogConnectorManager started.");
  }

  private void loadMetalake() {
    if (!catalogRegister.isTrinoStarted()) {
      LOG.info("Waiting for the Trino started.");
      return;
    }

    for (String usedMetalake : usedMetalakes) {
      try {
        GravitinoMetalake metalake =
            metalakes.computeIfAbsent(usedMetalake, this::retrieveMetalake);
        LOG.info("Load metalake: {}", usedMetalake);
        loadCatalogs(metalake);
      } catch (Exception e) {
        LOG.error("Load Metalake {} failed.", usedMetalake, e);
      }
    }
  }

  private GravitinoMetalake retrieveMetalake(String metalakeName) {
    try {
      return gravitinoClient.loadMetalake(metalakeName);
    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GRAVITINO_METALAKE_NOT_EXISTS, "Metalake " + metalakeName + " not exists.");
    }
  }

  private void loadCatalogs(GravitinoMetalake metalake) {
    NameIdentifier[] catalogNames;
    try {
      catalogNames = metalake.listCatalogs();
    } catch (Exception e) {
      LOG.error("Failed to list catalogs in metalake {}.", metalake.name(), e);
      return;
    }

    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));

    // Delete those catalogs that have been deleted in Gravitino server
    Set<String> catalogNameStrings =
        Arrays.stream(catalogNames)
            .map(
                id ->
                    config.simplifyCatalogNames()
                        ? id.name()
                        : getTrinoCatalogName(metalake.name(), id.name()))
            .collect(Collectors.toSet());

    for (Map.Entry<String, CatalogConnectorContext> entry : catalogConnectors.entrySet()) {
      if (!catalogNameStrings.contains(entry.getKey())
          &&
          // Skip the catalog doesn't belong to this metalake.
          entry.getValue().getMetalake().name().equals(metalake.name())) {
        try {
          unloadCatalog(entry.getValue().getCatalog());
        } catch (Exception e) {
          LOG.error("Failed to remove catalog {}.", entry.getKey(), e);
        }
      }
    }

    // Load new catalogs belows to the metalake.
    Arrays.stream(catalogNames)
        .forEach(
            (NameIdentifier nameIdentifier) -> {
              try {
                Catalog catalog = metalake.loadCatalog(nameIdentifier.name());
                GravitinoCatalog gravitinoCatalog = new GravitinoCatalog(metalake.name(), catalog);
                if (catalogConnectors.containsKey(getTrinoCatalogName(gravitinoCatalog))) {
                  // Reload catalogs that have been updated in Gravitino server.
                  reloadCatalog(gravitinoCatalog);
                } else {
                  if (catalog.type() == Catalog.Type.RELATIONAL) {
                    loadCatalog(gravitinoCatalog);
                  }
                }
              } catch (Exception e) {
                LOG.error(
                    "Failed to load metalake {}'s catalog {}.", metalake.name(), nameIdentifier, e);
              }
            });
  }

  private void reloadCatalog(GravitinoCatalog catalog) {
    String catalogFullName = getTrinoCatalogName(catalog);
    GravitinoCatalog oldCatalog = catalogConnectors.get(catalogFullName).getCatalog();
    if (catalog.getLastModifiedTime() <= oldCatalog.getLastModifiedTime()) {
      return;
    }

    catalogRegister.unregisterCatalog(catalogFullName);
    catalogConnectors.remove(catalogFullName);

    loadCatalogImpl(catalog);
    LOG.info("Update catalog '{}' in metalake {} successfully.", catalog, catalog.getMetalake());
  }

  private void loadCatalog(GravitinoCatalog catalog) {
    loadCatalogImpl(catalog);
    LOG.info("Load catalog {} in metalake {} successfully.", catalog, catalog.getMetalake());
  }

  private void loadCatalogImpl(GravitinoCatalog catalog) {
    try {
      catalogRegister.registerCatalog(getTrinoCatalogName(catalog), catalog);
    } catch (Exception e) {
      String message =
          String.format("Failed to create internal catalog connector. The catalog is: %s", catalog);
      LOG.error(message, e);
      throw new TrinoException(GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR, message, e);
    }
  }

  private void unloadCatalog(GravitinoCatalog catalog) {
    String catalogFullName = getTrinoCatalogName(catalog);
    catalogRegister.unregisterCatalog(catalogFullName);
    catalogConnectors.remove(catalogFullName);
    LOG.info(
        "Remove catalog '{}' in metalake {} successfully.",
        catalog.getName(),
        catalog.getMetalake());
  }

  public CatalogConnectorContext getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  public boolean catalogConnectorExist(String catalogName) {
    return catalogConnectors.containsKey(catalogName);
  }

  public List<GravitinoCatalog> getCatalogs() {
    return catalogConnectors.values().stream().map(CatalogConnectorContext::getCatalog).toList();
  }

  public void shutdown() {
    LOG.info("Gravitino CatalogConnectorManager shutdown.");
    throw new NotImplementedException();
  }

  public String getTrinoCatalogName(String metalake, String catalog) {
    return config.simplifyCatalogNames() ? catalog : String.format("\"%s.%s\"", metalake, catalog);
  }

  public String getTrinoCatalogName(GravitinoCatalog catalog) {
    return getTrinoCatalogName(catalog.getMetalake(), catalog.getName());
  }

  public void addMetalake(String metalake) {
    if (config.simplifyCatalogNames() && usedMetalakes.size() > 1)
      throw new TrinoException(
          GRAVITINO_MISSING_CONFIG,
          "Multiple metalakes are not supported when setting gravitino.simplify-catalog-names = true");
    usedMetalakes.add(metalake);
  }

  public Set<String> getUsedMetalakes() {
    return usedMetalakes;
  }

  public Connector createConnector(
      String connectorName, GravitinoConfig config, ConnectorContext context) {
    try {
      String catalogConfig = config.getCatalogConfig();

      GravitinoCatalog catalog = GravitinoCatalog.fromJson(catalogConfig);
      CatalogConnectorContext.Builder builder =
          catalogConnectorFactory.createCatalogConnectorContextBuilder(catalog);
      builder
          .withMetalake(metalakes.computeIfAbsent(catalog.getMetalake(), this::retrieveMetalake))
          .withContext(context);

      CatalogConnectorContext connectorContext = builder.build();
      catalogConnectors.put(connectorName, connectorContext);
      LOG.info("Create connector {} successful", connectorName);
      return connectorContext.getConnector();
    } catch (Exception e) {
      LOG.error("Failed to create connector: {}", connectorName, e);
      throw new TrinoException(
          GRAVITINO_OPERATION_FAILED, "Failed to create connector: " + connectorName, e);
    }
  }

  public void loadMetalakeSync() throws Exception {
    Future<?> future = executorService.submit(this::loadMetalake);
    future.get(LOAD_METALAKE_TIMEOUT, TimeUnit.SECONDS);
  }

  public GravitinoMetalake getMetalake(String metalake) {
    if (!usedMetalakes.contains(metalake)) {
      throw new TrinoException(
          GRAVITINO_OPERATION_FAILED,
          "This connector does not allowed to access metalake " + metalake);
    }
    return metalakes.computeIfAbsent(metalake, this::retrieveMetalake);
  }
}
