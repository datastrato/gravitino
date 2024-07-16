/*
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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is generated when an attempt to drop a schema fails due to an exception.
 */
@DeveloperApi
public final class DropSchemaFailureEvent extends SchemaFailureEvent {
  private final boolean cascade;

  public DropSchemaFailureEvent(
      String user, NameIdentifier identifier, Exception exception, boolean cascade) {
    super(user, identifier, exception);
    this.cascade = cascade;
  }

  /**
   * Indicates whether the drop operation was performed with a cascade option.
   *
   * @return A boolean value indicating whether the drop operation was set to cascade. If {@code
   *     true}, dependent objects such as tables and views within the schema were also dropped.
   *     Otherwise, the operation would fail if the schema contained any dependent objects.
   */
  public boolean cascade() {
    return cascade;
  }
}
