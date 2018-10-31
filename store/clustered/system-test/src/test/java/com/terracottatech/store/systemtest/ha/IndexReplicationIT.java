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
package com.terracottatech.store.systemtest.ha;

import com.terracottatech.store.Type;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.common.test.Employee;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static com.terracottatech.tool.WaitForAssert.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class IndexReplicationIT extends HABase {

  public IndexReplicationIT() {
    super(ConfigType.OFFHEAP_ONLY);
  }

  @Test
  public void testIndexDestroyReplication() throws Exception {
    Random random = new Random();

    random.ints(1000).forEach(x -> {
      employeeWriterReader.add(x,
              Employee.NAME.newCell("Name" + x),
              Employee.BONUS.newCell((double) (x * 100)),
              Employee.CURRENT.newCell(random.nextBoolean()),
              Employee.CELL_NUMBER.newCell((long) x),
              Employee.GENDER.newCell(random.nextBoolean() == true ? 'M' : 'F'),
              Employee.SSN.newCell(x));
    });

    Indexing indexing = dataset.getIndexing();

    indexing.createIndex(Employee.NAME, IndexSettings.BTREE);
    indexing.createIndex(Employee.BONUS, IndexSettings.BTREE);
    indexing.createIndex(Employee.CURRENT, IndexSettings.BTREE);
    indexing.createIndex(Employee.CELL_NUMBER, IndexSettings.BTREE);
    indexing.createIndex(Employee.GENDER, IndexSettings.BTREE);
    indexing.createIndex(Employee.SSN, IndexSettings.BTREE);

    assertThatEventually(indexing::getLiveIndexes, hasSize(6)).within(Duration.ofSeconds(5));

    indexing.getLiveIndexes().forEach(indexing::destroyIndex);

    CLUSTER.getClusterControl().terminateActive();

    assertThat(indexing.getLiveIndexes().size(), Matchers.is(0));
    assertThat(indexing.getAllIndexes().size(), Matchers.is(0));

  }

  @Test
  public void testIndexReplication() throws Exception {

    CLUSTER.getClusterControl().terminateOnePassive();

    dataset.close();

    datasetManager.newDataset("test-with-Index", Type.INT,
        getDatasetBuilder()
                    .index(Employee.CELL_NUMBER, IndexSettings.BTREE)
                    .build());
    dataset = datasetManager.getDataset("test-with-Index", Type.INT);

    Indexing indexing = dataset.getIndexing();

    indexing.createIndex(Employee.NAME, IndexSettings.BTREE).get();
    indexing.createIndex(Employee.BONUS, IndexSettings.BTREE).get();

    assertThat(indexing.getLiveIndexes().size(), Matchers.is(3));

    CLUSTER.getClusterControl().startOneServer();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    assertThat(indexing.getLiveIndexes().size(), Matchers.is(3));

    dataset.close();
  }

  @Test
  public void testIndexReplicationDuringFailover() throws Exception {

    CompletableFuture<?>[] futures = new CompletableFuture<?>[5];

    ExecutorService executorService = Executors.newFixedThreadPool(5);
    for (int i = 0; i < 5; i++) {
      futures[i] = CompletableFuture.runAsync(() -> {
        Random random = ThreadLocalRandom.current();
        random.ints(8000).forEach(x -> {
          employeeWriterReader.add(x,
                  Employee.NAME.newCell("Name" + x),
                  Employee.BONUS.newCell((double) (x * 100)),
                  Employee.CURRENT.newCell(random.nextBoolean()),
                  Employee.CELL_NUMBER.newCell((long) x));
        });
      }, executorService);
    }

    CompletableFuture.allOf(futures).get();

    Operation<Index<String>> index = dataset.getIndexing().createIndex(Employee.NAME, IndexSettings.BTREE);

    CompletableFuture<Index<String>> indexCompletableFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return index.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    });

    Thread.sleep(500);

    CLUSTER.getClusterControl().terminateActive();

    Index<String> stringIndex = indexCompletableFuture.get();

    assertThat(stringIndex.status(), Matchers.is(Index.Status.LIVE));
    assertThat(dataset.getIndexing().getLiveIndexes().size(), Matchers.is(1));

  }

}
