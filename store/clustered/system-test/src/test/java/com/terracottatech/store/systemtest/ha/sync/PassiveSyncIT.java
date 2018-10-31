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

import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.systemtest.ha.HABase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class PassiveSyncIT extends HABase {

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);

  @Parameterized.Parameters(name = "{0}")
  public static ConfigType[] datasets() {
    return new ConfigType[] {ConfigType.OFFHEAP_ONLY, ConfigType.OFFHEAP_DISK};
  }

  public PassiveSyncIT(ConfigType configType) {
    super(configType);
  }

  @Override
  protected DatasetConfigurationBuilder getDatasetBuilder() {
    return super.getDatasetBuilder().index(nameCell, IndexSettings.BTREE);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    CLUSTER.getClusterControl().terminateOnePassive();

    Random random = new Random();
    random.ints(10000).forEach( x -> {
      employeeWriterReader.add(x, nameCell.newCell("Name" + x), phoneCell.newCell(x));
    });
  }

  @Test
  public void testPassiveSync() throws Exception {

    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    List<Record<Integer>> records = new ArrayList<>();
    employeeWriterReader.records().forEach(records::add);

    assertThat(dataset.getIndexing().getLiveIndexes().size(), is(1));

    CLUSTER.getClusterControl().terminateActive();

    Iterator<Record<Integer>> recordIterator = records.iterator();

    // Tests the order of records visited is same with same data
    employeeWriterReader.records().forEach(x -> {
      Record<Integer> next = recordIterator.next();
      assertThat(x.getKey(), is(next.getKey()));
      assertThat(x.get(nameCell).get(), equalTo(next.get(nameCell).get()));
      assertThat(x.get(phoneCell).get(), equalTo(next.get(phoneCell).get()));
    });

    CompletableFuture.runAsync(() -> {
      while (true) {
        if (dataset.getIndexing().getLiveIndexes().size() == 1) {
          break;
        }
      }
    }).get(5, TimeUnit.SECONDS);

  }

  @Test
  public void testPassiveRestartAfterSync() throws Exception {
    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    CLUSTER.getClusterControl().terminateOnePassive();

    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

  }
}
