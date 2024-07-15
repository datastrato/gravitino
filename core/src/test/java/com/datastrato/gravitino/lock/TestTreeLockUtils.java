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

package com.datastrato.gravitino.lock;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.apache.gravitino.NameIdentifier;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;

public class TestTreeLockUtils {

  @Test
  void testHolderMultipleLock() throws Exception {
    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    TreeLockUtils.doWithTreeLock(
        NameIdentifier.of("test"),
        LockType.READ,
        () ->
            TreeLockUtils.doWithTreeLock(
                NameIdentifier.of("test", "test1"), LockType.WRITE, () -> null));
  }
}
