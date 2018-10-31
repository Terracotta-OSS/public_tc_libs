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

package com.terracottatech.store.systemtest;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.builder.datasetconfiguration.ClusteredDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.testing.rules.EnterpriseCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.terracottatech.store.CellSet.of;

/**
 * Test for TDB-2789
 *
 * Test to reproduce memory leak by non portable mutative streams. Context entries on thread local of sovereign finalizer
 * store are being added by a TCStore thread and then a Voltron thread is trying to remove the entries when closing the
 * stream which causes this memory leak and OOM.
 *
 * @author Eugene Sebastian
 */
@Ignore
public class MemoryLeakNonPortableStreamIT extends BaseSystemTest {

  @ClassRule
  public static EnterpriseCluster CLUSTER = initCluster("OffheapAndFRS.xmlfrag");

  private static final IntCellDefinition CELL_1 = CellDefinition.defineInt("cell1");

  @Before
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();

  }


  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateAllServers();
  }

  @Test
  public void test1() throws Exception {
    try (DatasetManager manager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration configuration = new ClusteredDatasetConfigurationBuilder()
          .offheap(CLUSTER_OFFHEAP_RESOURCE)
          .build();
      manager.newDataset("store-0", Type.INT, configuration);
      Dataset<Integer> dataset = manager.getDataset("store-0", Type.INT);
      DatasetWriterReader<Integer> writerReader = dataset.writerReader();
      dataset.getIndexing().createIndex(CELL_1, IndexSettings.BTREE).get();
      int numRecords = 1_000_000;
      int numThreads = Runtime.getRuntime().availableProcessors();
      ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

      //load
      for (Future<Void> f : load(writerReader, numRecords, numThreads, service)) {
        f.get();
      }

      //mutative streams which leak memory
      for (Future<Void> f : mutate(writerReader, numRecords, numThreads, service)) {
        f.get();
      }

      service.shutdown();

    }
  }

  private List<Future<Void>> load(DatasetWriterReader<Integer> writerReader, int numRecords, int numThreads, ExecutorService service) {
    List<Future<Void>> futures = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < numThreads; ++i) {
      futures.add(service.submit(() -> {
        for (; ; ) {
          int recordId = counter.incrementAndGet();
          if (recordId > numRecords) break;
          writerReader.add(recordId, CELL_1.newCell(recordId));
          if (recordId % 10_000 == 0) {
            System.out.println("Added " + recordId);
          }
        }
        return null;
      }));
    }
    return futures;
  }

  private List<Future<Void>> mutate(DatasetWriterReader<Integer> writerReader, int numRecords, int numThreads, ExecutorService service) {
    List<Future<Void>> futures = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < numThreads; ++i) {
      futures.add(service.submit(() -> {
        while (true) {
          int recordId = counter.incrementAndGet();
          if (recordId > numRecords) break;
          writerReader.records().filter(CELL_1.value().is(recordId)).mutate(r -> of(CELL_1.newCell(recordId + numRecords)));
          if (recordId % 1000 == 0) {
            System.out.println("Mutated " + recordId);
          }

        }
        return null;
      }));
    }

    return futures;
  }
}