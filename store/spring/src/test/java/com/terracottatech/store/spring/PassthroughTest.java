/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.store.spring;


import com.terracottatech.store.client.DatasetEntityClientService;
import com.terracottatech.store.server.DatasetEntityServerService;
import com.terracottatech.testing.AbstractEnterprisePassthroughTest;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

public class PassthroughTest extends AbstractEnterprisePassthroughTest {

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    OffheapResourcesType offheapResourcesType = new OffheapResourcesType();
    addResourceType(offheapResourcesType, "offheap", 1, MemoryUnit.MB);

    return server -> {
      server.registerExtendedConfiguration(new OffHeapResourcesProvider(offheapResourcesType));
      server.registerClientEntityService(new DatasetEntityClientService());
      server.registerServerEntityService(new DatasetEntityServerService());
    };
  }

  @Override
  protected List<String> provideStripeNames() {
    return Collections.singletonList("stripe");
  }

  private void addResourceType(OffheapResourcesType offheapResourcesType, String name, long value, MemoryUnit unit) {
    ResourceType resourceType = new ResourceType();
    resourceType.setName(name);
    resourceType.setValue(BigInteger.valueOf(value));
    resourceType.setUnit(unit);

    offheapResourcesType.getResource().add(resourceType);
  }
}
