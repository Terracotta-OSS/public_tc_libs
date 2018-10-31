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

package com.terracottatech.store.systemtest.ha.sync;

import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.systemtest.ha.HABase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PassiveReplicationWhileSyncIT extends HABase {

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);

  private Set<Integer> keys = new HashSet<>();

  @Parameterized.Parameters(name = "{0}")
  public static ConfigType[] datasets() {
    return new ConfigType[] {ConfigType.OFFHEAP_ONLY, ConfigType.OFFHEAP_DISK};
  }

  public PassiveReplicationWhileSyncIT(ConfigType configType) {
    super(configType);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    CLUSTER.getClusterControl().terminateOnePassive();

    Random random = new Random();
    random.ints().distinct().limit(10000).forEach( x -> {
      keys.add(x);
      boolean added = employeeWriterReader.add(x, nameCell.newCell("Name" + x), phoneCell.newCell(x));
      assertTrue(added);
    });
  }

  @Test
  public void testPassiveReplicationWhileSync() throws Exception {

    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      Random random = new Random();
      random.ints().distinct().filter(i -> !keys.contains(i)).limit(10000).forEach(x -> {
        keys.add(x);
        boolean added = employeeWriterReader.add(x, nameCell.newCell("Name" + x), phoneCell.newCell(x));
        assertTrue(added);
      });
    });

    Thread.sleep(200);

    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    future.get();

    CLUSTER.getClusterControl().terminateActive();

    employeeWriterReader.records().forEach(x -> assertThat(keys.contains(x.getKey()), is(true)));
  }
}
