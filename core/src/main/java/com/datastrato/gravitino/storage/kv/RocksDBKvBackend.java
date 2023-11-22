/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.utils.ByteUtils;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RocksDBKvBackend} is a RocksDB implementation of KvBackend interface. If we want to use
 * another kv implementation, We can just implement {@link KvBackend} interface and use it in the
 * Gravitino.
 */
public class RocksDBKvBackend implements KvBackend {
  public static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKvBackend.class);
  private TransactionDB db;

  /**
   * Initialize the RocksDB backend instance. We have used the {@link TransactionDB} to support
   * transaction instead of {@link RocksDB} instance.
   */
  private TransactionDB initRocksDB(Config config) throws RocksDBException {
    RocksDB.loadLibrary();
    final Options options = new Options();
    options.setCreateIfMissing(true);

    String dbPath = config.get(Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH);
    File dbDir = new File(dbPath, "instance");
    try {
      if (!dbDir.exists() && !dbDir.mkdirs()) {
        throw new RocksDBException(
            String.format("Can't create RocksDB path '%s'", dbDir.getAbsolutePath()));
      }
      LOGGER.info("Rocksdb storage directory:{}", dbDir);
      // TODO (yuqi), make options and transactionDBOptions configurable
      TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
      return TransactionDB.open(options, transactionDBOptions, dbDir.getAbsolutePath());
    } catch (RocksDBException ex) {
      LOGGER.error(
          "Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
          ex.getCause(),
          ex.getMessage(),
          ex.getStackTrace());
      throw ex;
    }
  }

  @Override
  public void initialize(Config config) throws IOException {
    try {
      db = initRocksDB(config);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(byte[] key, byte[] value, boolean overwrite) throws IOException {
    try {
      handlePutWithoutTransaction(key, value, overwrite);
    } catch (EntityAlreadyExistsException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void handlePutWithoutTransaction(byte[] key, byte[] value, boolean overwrite)
      throws RocksDBException {
    if (overwrite) {
      db.put(key, value);
      return;
    }
    byte[] existKey = db.get(key);
    if (existKey != null) {
      throw new EntityAlreadyExistsException(
          String.format(
              "Key %s already exists in the database, please use overwrite option to overwrite it",
              ByteUtils.formatByteArray(key)));
    }
    db.put(key, value);
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    try {
      return db.get(key);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<Pair<byte[], byte[]>> scan(KvRangeScan scanRange) throws IOException {
    RocksIterator rocksIterator = db.newIterator();
    rocksIterator.seek(scanRange.getStart());

    List<Pair<byte[], byte[]>> result = Lists.newArrayList();
    int count = 0;
    while (count < scanRange.getLimit() && rocksIterator.isValid()) {
      byte[] key = rocksIterator.key();

      // Break if the key is out of the scan range
      if (Bytes.wrap(key).compareTo(scanRange.getEnd()) > 0) {
        break;
      }

      if (!scanRange.getPredicate().test(key, rocksIterator.value())) {
        rocksIterator.next();
        continue;
      }

      if (Bytes.wrap(key).compareTo(scanRange.getStart()) == 0) {
        if (scanRange.isStartInclusive()) {
          result.add(Pair.of(key, rocksIterator.value()));
          count++;
        }
      } else if (Bytes.wrap(key).compareTo(scanRange.getEnd()) == 0) {
        if (scanRange.isEndInclusive()) {
          result.add(Pair.of(key, rocksIterator.value()));
        }
        break;
      } else {
        result.add(Pair.of(key, rocksIterator.value()));
        count++;
      }

      rocksIterator.next();
    }

    rocksIterator.close();
    return result;
  }

  @Override
  public boolean delete(byte[] key) throws IOException {
    try {
      db.delete(key);
      return true;
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean deleteRange(KvRangeScan deleteRange) throws IOException {
    RocksIterator rocksIterator = db.newIterator();
    rocksIterator.seek(deleteRange.getStart());

    while (rocksIterator.isValid()) {
      byte[] key = rocksIterator.key();
      // Break if the key is out of the scan range
      if (Bytes.wrap(key).compareTo(deleteRange.getEnd()) > 0) {
        break;
      }

      if (Bytes.wrap(key).compareTo(deleteRange.getStart()) == 0) {
        if (deleteRange.isStartInclusive()) {
          delete(key);
        }
      } else if (Bytes.wrap(key).compareTo(deleteRange.getEnd()) == 0) {
        if (deleteRange.isEndInclusive()) {
          delete(key);
        }
        break;
      } else {
        delete(key);
      }

      rocksIterator.next();
    }

    rocksIterator.close();
    return true;
  }

  @Override
  public void close() throws IOException {
    db.close();
  }
}
