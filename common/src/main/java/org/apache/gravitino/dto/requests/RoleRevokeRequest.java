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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to revoke roles from the user or the group. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class RoleRevokeRequest implements RESTRequest {
  @JsonProperty("roleNames")
  private final List<String> roleNames;

  /**
   * Constructor for RoleRevokeRequest.
   *
   * @param roleNames The roleName for the RoleRevokeRequest.
   */
  public RoleRevokeRequest(List<String> roleNames) {
    this.roleNames = roleNames;
  }

  /** Default constructor for RoleRevokeRequest. */
  public RoleRevokeRequest() {
    this(null);
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if the role names is not set or empty.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        roleNames != null && !roleNames.isEmpty(),
        "\"roleName\" field is required and cannot be empty");
  }
}
