/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

// Referred from Apache Iceberg's OAuthTokenResponse implementation
// core/src/main/java/org/apache/iceberg/rest/auth/responses/OAuthTokenResponse.java
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class OAuthTokenResponse extends BaseResponse {
  @JsonProperty("access_token")
  private final String accessToken;

  @Nullable
  @JsonProperty("issued_token_type")
  private final String issuedTokenType;

  @JsonProperty("token_type")
  private final String tokenType;

  @JsonProperty("expires_in")
  private final Integer expiresIn;

  @JsonProperty("scope")
  private final String scope;

  @Nullable
  @JsonProperty("refresh_token")
  private final String refreshToken;

  public OAuthTokenResponse(
      String accessToken,
      String issuedTokenType,
      String tokenType,
      Integer expiresIn,
      String scope,
      String refreshToken) {
    this.accessToken = accessToken;
    this.issuedTokenType = issuedTokenType;
    this.tokenType = tokenType;
    this.expiresIn = expiresIn;
    this.scope = scope;
    this.refreshToken = refreshToken;
  }

  // This is the constructor that is used by Jackson deserializer
  public OAuthTokenResponse() {
    super();
    this.accessToken = null;
    this.issuedTokenType = null;
    this.tokenType = null;
    this.expiresIn = null;
    this.scope = null;
    this.refreshToken = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(accessToken), "Invalid access token: null");
    Preconditions.checkArgument(
        "bearer".equalsIgnoreCase(tokenType),
        "Unsupported token type: %s (must be \"bearer\")",
        tokenType);
  }
}
