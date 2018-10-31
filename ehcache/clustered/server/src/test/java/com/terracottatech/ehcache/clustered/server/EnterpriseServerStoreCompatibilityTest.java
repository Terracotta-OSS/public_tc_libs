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

package com.terracottatech.ehcache.clustered.server;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;
import org.ehcache.clustered.server.ServerStoreCompatibility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;

public class EnterpriseServerStoreCompatibilityTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testRestartableDedicatedPoolCompatibility() throws Exception {
    PoolAllocation poolAllocation1 = new EnterprisePoolAllocation.DedicatedRestartable("resource1", 10L, 0);
    ServerStoreConfiguration configuration1 = new ServerStoreConfiguration(poolAllocation1,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    PoolAllocation poolAllocation2 = new EnterprisePoolAllocation.DedicatedRestartable("resource2", 10L, 0);
    ServerStoreConfiguration configuration2 = new ServerStoreConfiguration(poolAllocation2,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    ServerStoreCompatibility compatibility = new ServerStoreCompatibility();
    expectedException.expect(InvalidServerStoreConfigurationException.class);
    expectedException.expectMessage("resourcePoolType");
    compatibility.verify(configuration1, configuration2);
  }

  @Test
  public void testRestartableDedicatedAndUnknownPoolCompatibility() throws Exception {
    PoolAllocation poolAllocation1 = new EnterprisePoolAllocation.DedicatedRestartable("resource1", 10L, 0);
    ServerStoreConfiguration configuration1 = new ServerStoreConfiguration(poolAllocation1,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    PoolAllocation poolAllocation2 = new PoolAllocation.Unknown();
    ServerStoreConfiguration configuration2 = new ServerStoreConfiguration(poolAllocation2,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    ServerStoreCompatibility compatibility = new ServerStoreCompatibility();
    compatibility.verify(configuration1, configuration2);
  }

  @Test
  public void testDedicatedAndDedicatedRestartablePoolCompatibility() throws Exception {
    PoolAllocation poolAllocation1 = new EnterprisePoolAllocation.DedicatedRestartable("resource1", 10L, 0);
    ServerStoreConfiguration configuration1 = new ServerStoreConfiguration(poolAllocation1,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    PoolAllocation poolAllocation2 = new PoolAllocation.Dedicated("resource1", 10L);
    ServerStoreConfiguration configuration2 = new ServerStoreConfiguration(poolAllocation2,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    ServerStoreCompatibility compatibility = new ServerStoreCompatibility();
    expectedException.expect(InvalidServerStoreConfigurationException.class);
    expectedException.expectMessage("resourcePoolType");
    compatibility.verify(configuration1, configuration2);
  }

  @Test
  public void testRestartableSharedPoolCompatibility() throws Exception {
    PoolAllocation poolAllocation1 = new EnterprisePoolAllocation.SharedRestartable("resource1");
    ServerStoreConfiguration configuration1 = new ServerStoreConfiguration(poolAllocation1,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    PoolAllocation poolAllocation2 = new EnterprisePoolAllocation.SharedRestartable("resource2");
    ServerStoreConfiguration configuration2 = new ServerStoreConfiguration(poolAllocation2,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    ServerStoreCompatibility compatibility = new ServerStoreCompatibility();
    expectedException.expect(InvalidServerStoreConfigurationException.class);
    expectedException.expectMessage("resourcePoolType");
    compatibility.verify(configuration1, configuration2);
  }

  @Test
  public void testRestartableSharedAndUnknownPoolCompatibility() throws Exception {
    PoolAllocation poolAllocation1 = new EnterprisePoolAllocation.SharedRestartable("resource1");
    ServerStoreConfiguration configuration1 = new ServerStoreConfiguration(poolAllocation1,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    PoolAllocation poolAllocation2 = new PoolAllocation.Unknown();
    ServerStoreConfiguration configuration2 = new ServerStoreConfiguration(poolAllocation2,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    ServerStoreCompatibility compatibility = new ServerStoreCompatibility();
    compatibility.verify(configuration1, configuration2);
  }

  @Test
  public void testSharedAndSharedRestartablePoolCompatibility() throws Exception {
    PoolAllocation poolAllocation1 = new PoolAllocation.Shared("resource1");
    ServerStoreConfiguration configuration1 = new ServerStoreConfiguration(poolAllocation1,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    PoolAllocation poolAllocation2 = new EnterprisePoolAllocation.SharedRestartable("resource1");
    ServerStoreConfiguration configuration2 = new ServerStoreConfiguration(poolAllocation2,
        String.class.getName(), String.class.getName(), String.class.getName(), String.class.getName(), Consistency.EVENTUAL);

    ServerStoreCompatibility compatibility = new ServerStoreCompatibility();
    expectedException.expect(InvalidServerStoreConfigurationException.class);
    expectedException.expectMessage("resourcePoolType");
    compatibility.verify(configuration1, configuration2);
  }

}
