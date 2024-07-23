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
package org.apache.gravitino.lifecycle;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * LifecycleHooks provide the ability to execute specific hook actions before or after calling
 * specific methods. Now we only support the post hook.
 */
public class LifecycleHooks {

  private final Map<String, List<BiConsumer>> postHookMap = Maps.newHashMap();

  public void addPostHook(String method, BiConsumer hook) {
    List<BiConsumer> postHooks = postHookMap.computeIfAbsent(method, key -> Lists.newArrayList());
    postHooks.add(hook);
  }

  public boolean isEmpty() {
    return postHookMap.isEmpty();
  }

  List<BiConsumer> getPostHooks(String method) {
    List<BiConsumer> postHooks = postHookMap.get(method);
    if (postHooks == null) {
      return Collections.emptyList();
    }
    return postHooks;
  }
}
