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
package org.apache.gravitino.integration.test;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class UserGroupIT extends AbstractIT {

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();
  }

  @Test
  public void testListUsers() {
    client.createMetalake("listMetalake", "", Collections.emptyMap());
    client.addUser("listMetalake", "user1");
    client.addUser("listMetalake", "user2");
    Assertions.assertArrayEquals(
        new String[] {"user1", "user2"}, client.listUserNames("listMetalake"));
    User[] users = client.listUsers("listMetalake");
    Assertions.assertEquals(2, users.length);
    Assertions.assertEquals("user1", users[0].name());
    Assertions.assertEquals("user2", users[1].name());
  }
}
