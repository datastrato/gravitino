/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.event;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.DeveloperApi;

/**
 * The abstract base class for all events. It encapsulates common information such as the user who
 * generated the event and the identifier for the resource associated with the event. Subclasses
 * should provide specific details related to their individual event types.
 */
@DeveloperApi
public abstract class Event {
  private String user;
  private NameIdentifier identifier;

  /**
   * Constructs an Event instance with the specified user and resource identifier details.
   *
   * @param user The user associated with this event. It provides context about who triggered the
   *     event.
   * @param identifier The resource identifier associated with this event. This may refer to various
   *     types of resources such as a metalake, catalog, schema, or table, etc.
   */
  protected Event(String user, NameIdentifier identifier) {
    this.user = user;
    this.identifier = identifier;
  }

  // Private default constructor to prevent instantiation without required parameters.
  private Event() {}

  /**
   * Retrieves the user associated with this event.
   *
   * @return A string representing the user associated with this event.
   */
  public String getUser() {
    return user;
  }

  /**
   * Retrieves the resource identifier associated with this event.
   *
   * @return A NameIdentifier object that represents the resource, like a metalake, catalog, schema,
   *     table, etc., associated with the event.
   */
  public NameIdentifier getIdentifier() {
    return identifier;
  }
}
