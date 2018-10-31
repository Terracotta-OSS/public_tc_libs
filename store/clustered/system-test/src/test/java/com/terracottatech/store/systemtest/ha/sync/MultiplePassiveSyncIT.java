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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.systemtest.BaseSystemTest;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MultiplePassiveSyncIT extends BaseSystemTest {

  @ClassRule
  public static EnterpriseCluster CLUSTER = initClusterWithPassive("OffheapAndFRS.xmlfrag", 3);

  private static final CellDefinition<String> nameCell = CellDefinition.define("name", Type.STRING);
  private static final CellDefinition<Integer> phoneCell = CellDefinition.define("phone", Type.INT);

  private DatasetWriterReader<Integer> writerReader;
  private Dataset<Integer> dataset;
  private DatasetManager clusteredDatasetManager;

  private List<Record<Integer>> records;

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();

    CLUSTER.getClusterControl().terminateOnePassive();
    CLUSTER.getClusterControl().terminateOnePassive();

    clusteredDatasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build();

    DatasetConfiguration datasetConfiguration = clusteredDatasetManager.datasetConfiguration()
            .offheap("primary-server-resource")
            .disk("cluster-disk-resource")
            .index(nameCell, IndexSettings.BTREE)
            .build();

    clusteredDatasetManager.newDataset("passiveSync", Type.INT, datasetConfiguration);
    dataset = clusteredDatasetManager.getDataset("passiveSync", Type.INT);

    writerReader = dataset.writerReader();

    Random random = new Random();

    random.ints(10000).forEach( x -> {
      writerReader.add(x, nameCell.newCell("Name" + x), phoneCell.newCell(x));
    });

    records = new LinkedList<>();

    writerReader.records().forEach(records::add);
  }

  @Test
  public void testMultiplePassiveSyncOneByOne() throws Exception {
    CLUSTER.getClusterControl().startOneServer();

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    CLUSTER.getClusterControl().startOneServer();

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    CLUSTER.getClusterControl().terminateActive();

    assertRecordVisitationOrder(records, writerReader);

    CLUSTER.getClusterControl().terminateActive();

    assertRecordVisitationOrder(records, writerReader);

  }

  @Test
  public void testMultiplePassiveSyncSimultaneously() throws Exception {
    CLUSTER.getClusterControl().startAllServers();

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    CLUSTER.getClusterControl().terminateActive();

    assertRecordVisitationOrder(records, writerReader);

    CLUSTER.getClusterControl().terminateActive();

    assertRecordVisitationOrder(records, writerReader);

  }

  private static void assertRecordVisitationOrder(Iterable<Record<Integer>> records, DatasetWriterReader<Integer> writerReader) {
    Iterator<Record<Integer>> iterator = records.iterator();
    writerReader.records().forEach(o -> {
      Record<Integer> next = iterator.next();
      assertThat(o.getKey(), is(next.getKey()));
      assertThat(o.get(nameCell).get(), is(next.get(nameCell).get()));
      assertThat(o.get(phoneCell).get(), is(next.get(phoneCell).get()));
    });
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    dataset.close();
    assertThat(clusteredDatasetManager.destroyDataset("passiveSync"), is(true));
  }

}
