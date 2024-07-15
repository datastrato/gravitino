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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a drop operation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class DropResponse extends BaseResponse {

  @JsonProperty("dropped")
  private final boolean dropped;

  /**
   * Constructor for DropResponse.
   *
   * @param dropped Whether the drop operation was successful.
   */
  public DropResponse(boolean dropped) {
    super(0);
    this.dropped = dropped;
  }

  /** Default constructor for DropResponse (used by Jackson deserializer). */
  public DropResponse() {
    super();
    this.dropped = false;
  }

  /**
   * Returns whether the drop operation was successful.
   *
   * @return True if the drop operation was successful, otherwise false.
   */
  public boolean dropped() {
    return dropped;
  }
}
