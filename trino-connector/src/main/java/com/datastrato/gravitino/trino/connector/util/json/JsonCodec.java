/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.util.json;

import com.datastrato.gravitino.trino.connector.GravitinoConnectorPluginManager;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.airlift.json.RecordAutoDetectModule;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class JsonCodec {
  private static ObjectMapper mapper;

  private static ObjectMapper buildMapper1(ClassLoader appClassLoader) {

    GravitinoConnectorPluginManager pluginManager = GravitinoConnectorPluginManager.instance(appClassLoader);
    Function<Object, String> nameResolver =
        obj -> {
          try {
            ClassLoader pluginClassloader = obj.getClass().getClassLoader();
            Field idField = pluginClassloader.getClass().getDeclaredField("pluginName");
            idField.setAccessible(true);
            String id = (String) idField.get(pluginClassloader);
            return id + ":" + obj.getClass().getName();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Function<String, Class<?>> classResolver =
        name -> {
          try {
            String[] nameParts = name.split(":");
            String id = nameParts[0];
            String className = nameParts[1];
            ClassLoader loader = pluginManager.getClassLoader(id);
            return loader.loadClass(className);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        };

    try {
      TypeManager typeManager = null;
      if (typeManager == null) {
        Class internalTypeManagerClass =
            appClassLoader.loadClass("io.trino.type.InternalTypeManager");
        Class typeRegistryClass = appClassLoader.loadClass("io.trino.metadata.TypeRegistry");
        Class typeOperatorsClass = appClassLoader.loadClass("io.trino.spi.type.TypeOperators");
        Class featuresConfigClass = appClassLoader.loadClass("io.trino.FeaturesConfig");

        Object typeRegistry =
            typeRegistryClass
                .getConstructor(typeOperatorsClass, featuresConfigClass)
                .newInstance(
                    typeOperatorsClass.getConstructor().newInstance(),
                    featuresConfigClass.getConstructor().newInstance());
        typeManager =
            (TypeManager)
                internalTypeManagerClass
                    .getConstructor(typeRegistryClass)
                    .newInstance(typeRegistry);
      }

      return buildMapper2(typeManager, nameResolver, classResolver);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static BlockEncodingSerde createBlockEncodingSerde(TypeManager typeManager) throws Exception {
    ClassLoader classLoader = typeManager.getClass().getClassLoader();
    Class blockEncodingManagerClass =
        classLoader.loadClass("io.trino.metadata.BlockEncodingManager");
    Class internalBlockEncodingSerdeClass =
        classLoader.loadClass("io.trino.metadata.InternalBlockEncodingSerde");
    return (BlockEncodingSerde)
        internalBlockEncodingSerdeClass
            .getConstructor(blockEncodingManagerClass, TypeManager.class)
            .newInstance(blockEncodingManagerClass.getConstructor().newInstance(), typeManager);
  }

  @SuppressWarnings("deprecation")
  private static ObjectMapper buildMapper2(
      TypeManager typeManger,
      Function<Object, String> nameResolver,
      Function<String, Class<?>> classResolver) {

    ObjectMapper objectMapper = new ObjectMapper();
    // ignore unknown fields (for backwards compatibility)
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    // do not allow converting a float to an integer
    objectMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
    // use ISO dates
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    // Skip fields that are null or absent (Optional) when serializing objects.
    // This only applies to mapped object fields, not containers like Map or List.
    objectMapper.setDefaultPropertyInclusion(
        JsonInclude.Value.construct(JsonInclude.Include.NON_ABSENT, JsonInclude.Include.ALWAYS));

    // disable auto detection of json properties... all properties must be explicit
    objectMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    objectMapper.disable(MapperFeature.USE_GETTERS_AS_SETTERS);
    objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
    objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
    objectMapper.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(
            ConnectorTransactionHandle.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(
            ConnectorTableHandle.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(ColumnHandle.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(ConnectorSplit.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(
            ConnectorInsertTableHandle.class, nameResolver, classResolver) {});

    SimpleModule module = new SimpleModule();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.registerModule(new GuavaModule());
    objectMapper.registerModule(new ParameterNamesModule());
    objectMapper.registerModule(new RecordAutoDetectModule());

    try {
      module.addDeserializer(Type.class, new TypeDeserializer(typeManger));
      module.addDeserializer(
          TypeSignature.class,
          new TypeSignatureDeserializer(typeManger.getClass().getClassLoader()));

      BlockEncodingSerde blockEncodingSerde = createBlockEncodingSerde(typeManger);
      module.addSerializer(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde));
      module.addDeserializer(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde));
    } catch (Exception e) {;
      throw new RuntimeException(e);
    }
    objectMapper.registerModule(module);
    return objectMapper;
  }

  public static ObjectMapper getMapper(ClassLoader appClassLoader) {
    if (mapper != null) {
      return mapper;
    }
    synchronized (JsonCodec.class) {
      mapper = buildMapper1(appClassLoader);
      return mapper;
    }
  }
}
