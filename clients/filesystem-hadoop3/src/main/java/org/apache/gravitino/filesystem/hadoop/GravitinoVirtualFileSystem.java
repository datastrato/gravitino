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
package org.apache.gravitino.filesystem.hadoop;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.file.BaseFilesetDataOperationCtx;
import org.apache.gravitino.file.ClientType;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetContext;
import org.apache.gravitino.file.FilesetDataOperation;
import org.apache.gravitino.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.shaded.com.google.common.base.Preconditions;
import org.apache.gravitino.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.gravitino.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GravitinoVirtualFileSystem} is a virtual file system which users can access `fileset` and
 * other resources. It obtains the actual storage location corresponding to the resource from the
 * Apache Gravitino server, and creates an independent file system for it to act as an agent for
 * users to access the underlying storage.
 */
public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger Logger = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);
  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private String metalakeName;
  private Cache<NameIdentifier, FilesetCatalog> catalogCache;
  private ScheduledThreadPoolExecutor catalogCleanScheduler;
  private Cache<String, FileSystem> filesetFSCache;
  private ScheduledThreadPoolExecutor filesetFSCleanScheduler;

  // The pattern is used to match gvfs path. The scheme prefix (gvfs://fileset) is optional.
  // The following path can be match:
  //     gvfs://fileset/fileset_catalog/fileset_schema/fileset1/file.txt
  //     /fileset_catalog/fileset_schema/fileset1/sub_dir/
  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$");

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }

    int maxCapacity =
        configuration.getInt(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT);
    Preconditions.checkArgument(
        maxCapacity > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY);

    long evictionMillsAfterAccess =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT);
    Preconditions.checkArgument(
        evictionMillsAfterAccess > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY);

    initializeFSCache(maxCapacity, evictionMillsAfterAccess);
    initializeCatalogCache();

    this.metalakeName =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    initializeClient(configuration);

    this.workingDirectory = new Path(name);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

    setConf(configuration);
    super.initialize(uri, getConf());
  }

  @VisibleForTesting
  Cache<String, FileSystem> getFilesetFSCache() {
    return filesetFSCache;
  }

  private void initializeFSCache(int maxCapacity, long expireAfterAccess) {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.filesetFSCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-fileset-cache-cleaner"));
    Caffeine<Object, Object> cacheBuilder =
        Caffeine.newBuilder()
            .maximumSize(maxCapacity)
            .scheduler(Scheduler.forScheduledExecutorService(filesetFSCleanScheduler))
            .removalListener(
                (key, value, cause) -> {
                  FileSystem fs = (FileSystem) value;
                  if (fs != null) {
                    try {
                      fs.close();
                    } catch (IOException e) {
                      Logger.error("Cannot close the file system for fileset: {}", key, e);
                    }
                  }
                });
    if (expireAfterAccess < 0) {
      cacheBuilder.expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS);
    }
    this.filesetFSCache = cacheBuilder.build();
  }

  private void initializeCatalogCache() {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.catalogCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-catalog-cache-cleaner"));
    // In most scenarios, it will not read so many catalog filesets at the same time, so we can just
    // set a default value for this cache.
    this.catalogCache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .scheduler(Scheduler.forScheduledExecutorService(catalogCleanScheduler))
            .build();
  }

  private ThreadFactory newDaemonThreadFactory(String name) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "-%d").build();
  }

  private void initializeClient(Configuration configuration) {
    // initialize the Gravitino client
    String serverUri =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(serverUri),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);

    String authType =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
            GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE);
    if (authType.equalsIgnoreCase(GravitinoVirtualFileSystemConfiguration.SIMPLE_AUTH_TYPE)) {
      this.client =
          GravitinoClient.builder(serverUri).withMetalake(metalakeName).withSimpleAuth().build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE)) {
      String authServerUri =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY,
          authServerUri);

      String credential =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY,
          credential);

      String path =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY,
          path);

      String scope =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.OAUTH2_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY,
          scope);

      DefaultOAuth2TokenProvider authDataProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(authServerUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();

      this.client =
          GravitinoClient.builder(serverUri)
              .withMetalake(metalakeName)
              .withOAuth(authDataProvider)
              .build();
    } else if (authType.equalsIgnoreCase(
        GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE)) {
      String principal =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY);
      checkAuthConfig(
          GravitinoVirtualFileSystemConfiguration.KERBEROS_AUTH_TYPE,
          GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY,
          principal);
      String keytabFilePath =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration
                  .FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY);
      KerberosTokenProvider authDataProvider;
      if (StringUtils.isNotBlank(keytabFilePath)) {
        // Using principal and keytab to create auth provider
        authDataProvider =
            KerberosTokenProvider.builder()
                .withClientPrincipal(principal)
                .withKeyTabFile(new File(keytabFilePath))
                .build();
      } else {
        // Using ticket cache to create auth provider
        authDataProvider = KerberosTokenProvider.builder().withClientPrincipal(principal).build();
      }
      this.client =
          GravitinoClient.builder(serverUri)
              .withMetalake(metalakeName)
              .withKerberosAuth(authDataProvider)
              .build();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported authentication type: %s for %s.",
              authType, GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY));
    }
  }

  private void checkAuthConfig(String authType, String configKey, String configValue) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue),
        "%s should not be null if %s is set to %s.",
        configKey,
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        authType);
  }

  private String getVirtualLocation(NameIdentifier identifier, boolean withScheme) {
    return String.format(
        "%s/%s/%s/%s",
        withScheme ? GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX : "",
        identifier.namespace().level(1),
        identifier.namespace().level(2),
        identifier.name());
  }

  private FileStatus convertFileStatusPathPrefix(
      FileStatus fileStatus, String actualPrefix, String virtualPrefix) {
    String filePath = fileStatus.getPath().toString();
    Preconditions.checkArgument(
        filePath.startsWith(actualPrefix),
        "Path %s doesn't start with prefix \"%s\".",
        filePath,
        actualPrefix);
    Path path = new Path(filePath.replaceFirst(actualPrefix, virtualPrefix));
    fileStatus.setPath(path);

    return fileStatus;
  }

  @VisibleForTesting
  NameIdentifier extractIdentifier(URI virtualUri) {
    String virtualPath = virtualUri.toString();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(virtualPath),
        "Uri which need be extracted cannot be null or empty.");

    Matcher matcher = IDENTIFIER_PATTERN.matcher(virtualPath);
    Preconditions.checkArgument(
        matcher.matches() && matcher.groupCount() == 3,
        "URI %s doesn't contains valid identifier",
        virtualPath);

    return NameIdentifier.of(metalakeName, matcher.group(1), matcher.group(2), matcher.group(3));
  }

  private FilesetContextPair getFilesetContext(Path virtualPath, FilesetDataOperation operation) {
    NameIdentifier identifier = extractIdentifier(virtualPath.toUri());
    String virtualPathString = virtualPath.toString();
    String subPath =
        virtualPathString.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)
            ? virtualPathString.substring(
                String.format(
                        "%s/%s/%s/%s",
                        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                        identifier.namespace().level(1),
                        identifier.namespace().level(2),
                        identifier.name())
                    .length())
            : virtualPathString.substring(
                String.format(
                        "/%s/%s/%s",
                        identifier.namespace().level(1),
                        identifier.namespace().level(2),
                        identifier.name())
                    .length());
    BaseFilesetDataOperationCtx requestCtx =
        BaseFilesetDataOperationCtx.builder()
            .withOperation(operation)
            .withSubPath(subPath)
            .withClientType(ClientType.HADOOP_GVFS)
            .build();
    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, identifier.namespace().level(1));
    FilesetCatalog filesetCatalog =
        catalogCache.get(
            catalogIdent, ident -> client.loadCatalog(catalogIdent.name()).asFilesetCatalog());
    Preconditions.checkArgument(
        filesetCatalog != null, String.format("Loaded fileset catalog: %s is null.", catalogIdent));

    FilesetContext context =
        filesetCatalog.getFilesetContext(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name()), requestCtx);

    String actualPath = context.actualPath();
    URI uri = new Path(actualPath).toUri();
    // we cache the fs for the same scheme, so we can reuse it
    FileSystem fs =
        filesetFSCache.get(
            uri.getScheme(),
            str -> {
              try {
                return FileSystem.newInstance(uri, getConf());
              } catch (IOException ioe) {
                throw new GravitinoRuntimeException(
                    "Exception occurs when create new FileSystem for actual uri: %s, msg: %s",
                    uri, ioe);
              }
            });

    return new FilesetContextPair(context, fs);
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public synchronized void setWorkingDirectory(Path newDir) {
    FilesetContextPair pair = getFilesetContext(newDir, FilesetDataOperation.SET_WORKING_DIR);
    Path actualPath = new Path(pair.getContext().actualPath());
    pair.getFileSystem().setWorkingDirectory(actualPath);
    this.workingDirectory = newDir;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.OPEN);
    Path actualPath = new Path(pair.getContext().actualPath());
    return pair.getFileSystem().open(actualPath, bufferSize);
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.CREATE);
    Path actualPath = new Path(pair.getContext().actualPath());
    return pair.getFileSystem()
        .create(actualPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.APPEND);
    Path actualPath = new Path(pair.getContext().actualPath());
    return pair.getFileSystem().append(actualPath, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // There are two cases that cannot be renamed:
    // 1. Fileset identifier is not allowed to be renamed, only its subdirectories can be renamed
    // which not in the storage location of the fileset;
    // 2. Fileset only mounts a single file, the storage location of the fileset cannot be renamed;
    // Otherwise the metadata in the Gravitino server may be inconsistent.
    NameIdentifier srcIdentifier = extractIdentifier(src.toUri());
    NameIdentifier dstIdentifier = extractIdentifier(dst.toUri());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path fileset identifier: %s should be same with src path fileset identifier: %s.",
        srcIdentifier,
        dstIdentifier);

    FilesetContextPair srcPair = getFilesetContext(src, FilesetDataOperation.RENAME);
    Path srcActualPath = new Path(srcPair.getContext().actualPath());

    FilesetContextPair dstPair = getFilesetContext(dst, FilesetDataOperation.RENAME);
    Path dstActualPath = new Path(dstPair.getContext().actualPath());

    return srcPair.getFileSystem().rename(srcActualPath, dstActualPath);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.DELETE);
    Path actualPath = new Path(pair.getContext().actualPath());
    return pair.getFileSystem().delete(actualPath, recursive);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.GET_FILE_STATUS);
    Path actualPath = new Path(pair.getContext().actualPath());
    FileStatus fileStatus = pair.getFileSystem().getFileStatus(actualPath);
    NameIdentifier identifier = extractIdentifier(path.toUri());
    return convertFileStatusPathPrefix(
        fileStatus,
        pair.getContext().fileset().storageLocation(),
        getVirtualLocation(identifier, true));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.LIST_STATUS);
    Path actualPath = new Path(pair.getContext().actualPath());
    FileStatus[] fileStatusResults = pair.getFileSystem().listStatus(actualPath);
    NameIdentifier identifier = extractIdentifier(path.toUri());
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                convertFileStatusPathPrefix(
                    fileStatus,
                    new Path(pair.getContext().fileset().storageLocation()).toString(),
                    getVirtualLocation(identifier, true)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    FilesetContextPair pair = getFilesetContext(path, FilesetDataOperation.MKDIRS);
    Path actualPath = new Path(pair.getContext().actualPath());
    return pair.getFileSystem().mkdirs(actualPath, permission);
  }

  @Override
  public short getDefaultReplication(Path f) {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_REPLICATION);
    Path actualPath = new Path(pair.getContext().actualPath());
    return pair.getFileSystem().getDefaultReplication(actualPath);
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    FilesetContextPair pair = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE);
    Path actualPath = new Path(pair.getContext().actualPath());
    return pair.getFileSystem().getDefaultBlockSize(actualPath);
  }

  @Override
  public synchronized void close() throws IOException {
    // close all actual FileSystems
    for (FileSystem fileSystem : filesetFSCache.asMap().values()) {
      try {
        fileSystem.close();
      } catch (IOException e) {
        // ignore
      }
    }
    filesetFSCache.invalidateAll();
    catalogCache.invalidateAll();

    // close the client
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // ignore
    }
    catalogCleanScheduler.shutdownNow();
    filesetFSCleanScheduler.shutdownNow();
    super.close();
  }

  private static class FilesetContextPair {
    private final FilesetContext context;
    private final FileSystem fileSystem;

    public FilesetContextPair(FilesetContext context, FileSystem fileSystem) {
      this.context = context;
      this.fileSystem = fileSystem;
    }

    public FilesetContext getContext() {
      return context;
    }

    public FileSystem getFileSystem() {
      return fileSystem;
    }
  }
}
