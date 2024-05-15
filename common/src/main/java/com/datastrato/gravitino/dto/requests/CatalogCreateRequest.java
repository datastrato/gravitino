/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogBasic;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to create a catalog. */
@Getter
@EqualsAndHashCode
@ToString
public class CatalogCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("type")
  private final CatalogBasic.Type type;

  @JsonProperty("provider")
  private final String provider;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for CatalogCreateRequest. */
  public CatalogCreateRequest() {
    this(null, null, null, null, null);
  }

  /**
   * Constructor for CatalogCreateRequest.
   *
   * @param name The name of the catalog.
   * @param type The type of the catalog.
   * @param provider The provider of the catalog.
   * @param comment The comment for the catalog.
   * @param properties The properties for the catalog.
   */
  public CatalogCreateRequest(
      String name,
      CatalogBasic.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.provider = provider;
    this.comment = comment;
    this.properties = properties;
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if name or type are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(type != null, "\"type\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(provider), "\"provider\" field is required and cannot be empty");
  }
}
