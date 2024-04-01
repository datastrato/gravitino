/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.impl;

import com.datastrato.gravitino.listener.EventListenerPlugin;
import com.datastrato.gravitino.listener.SupportsAsync;
import com.datastrato.gravitino.listener.SupportsAsync.Mode;
import com.datastrato.gravitino.utils.MapUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EventListenerManager loads listeners according to the configurations, and assemble the listeners
 * with following rules:
 *
 * <p>Wrap all listener with EventListenerWrapper to do some common process, like exception handing,
 * record metrics.
 *
 * <p>For async listeners with the shared dispatcher, will create a default AsyncQueueListener to
 * assemble the corresponding EventListenerWrappers.
 *
 * <p>For async listeners with the isolated dispatcher, will create a separate AsyncQueueListener
 * for each EventListenerWrapper.
 */
public class EventListenerManager {
  private static final Logger LOG = LoggerFactory.getLogger(EventListenerManager.class);
  public static final String GRAVITINO_EVENT_LISTENER_PREFIX = "gravitino.eventListener.";
  @VisibleForTesting static final String GRAVITINO_EVENT_LISTENER_NAMES = "names";
  @VisibleForTesting static final String GRAVITINO_EVENT_LISTENER_CLASSNAME = "className";
  private static final String GRAVITINO_EVENT_LISTENER_QUEUE_CAPACITY = "queueCapacity";
  private static final Splitter splitter = Splitter.on(",");
  private static final Joiner DOT = Joiner.on(".");

  private int queueCapacity;
  private List<EventListenerPlugin> eventListeners;

  public void init(Map<String, String> properties) {
    String queueCapacity = properties.get(GRAVITINO_EVENT_LISTENER_QUEUE_CAPACITY);
    this.queueCapacity =
        Optional.ofNullable(queueCapacity)
            .map(capacity -> Integer.valueOf(capacity))
            .orElse(Integer.valueOf(1000))
            .intValue();

    String eventListenerNames = properties.getOrDefault(GRAVITINO_EVENT_LISTENER_NAMES, "");
    Map<String, EventListenerPlugin> userEventListenerPlugins =
        splitter
            .omitEmptyStrings()
            .trimResults()
            .splitToStream(eventListenerNames)
            .collect(
                Collectors.toMap(
                    listenerName -> listenerName,
                    listenerName ->
                        loadUserEventListenerPlugin(
                            listenerName,
                            MapUtils.getPrefixMap(properties, DOT.join(listenerName, ""))),
                    (existingValue, newValue) -> {
                      throw new IllegalStateException(
                          "Duplicate event listener name detected: " + existingValue);
                    }));
    this.eventListeners = assembleEventListeners(userEventListenerPlugins);
  }

  public void start() {
    eventListeners.stream().forEach(listener -> listener.start());
  }

  public void stop() {
    eventListeners.stream().forEach(listener -> listener.stop());
  }

  public EventBus createEventBus() {
    return new EventBus(eventListeners);
  }

  private List<EventListenerPlugin> assembleEventListeners(
      Map<String, EventListenerPlugin> userEventListeners) {
    List<EventListenerPlugin> sharedQueueListeners = new ArrayList<>();

    List<EventListenerPlugin> listeners =
        userEventListeners.entrySet().stream()
            .map(
                entrySet -> {
                  String listenerName = entrySet.getKey();
                  EventListenerPlugin listener = entrySet.getValue();
                  if (listener instanceof SupportsAsync) {
                    SupportsAsync asyncListener = (SupportsAsync) listener;
                    if (Mode.SHARED.equals(asyncListener.asyncMode())) {
                      sharedQueueListeners.add(
                          new EventListenerPluginWrapper(listenerName, listener));
                      return null;
                    } else {
                      return new AsyncQueueListener(
                          ImmutableList.of(new EventListenerPluginWrapper(listenerName, listener)),
                          listenerName,
                          queueCapacity);
                    }
                  } else {
                    return new EventListenerPluginWrapper(listenerName, listener);
                  }
                })
            .filter(listener -> listener != null)
            .collect(Collectors.toList());

    if (sharedQueueListeners.size() > 0) {
      listeners.add(new AsyncQueueListener(sharedQueueListeners, "default", queueCapacity));
    }
    return listeners;
  }

  private EventListenerPlugin loadUserEventListenerPlugin(
      String listenerName, Map<String, String> config) {
    LOG.info("EventListener:{}, config:{}.", listenerName, config);
    String className = config.get(GRAVITINO_EVENT_LISTENER_CLASSNAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(className),
        String.format(
            "EventListener:%s, %s%s.%s is not set in configuration.",
            listenerName,
            GRAVITINO_EVENT_LISTENER_PREFIX,
            listenerName,
            GRAVITINO_EVENT_LISTENER_CLASSNAME));

    try {
      EventListenerPlugin listenerPlugin =
          (EventListenerPlugin) Class.forName(className).getDeclaredConstructor().newInstance();
      listenerPlugin.init(config);
      return listenerPlugin;
    } catch (Exception e) {
      LOG.error(
          "Failed to create and initialize event listener {}, className: {}.",
          listenerName,
          className,
          e);
      throw new RuntimeException(e);
    }
  }
}
