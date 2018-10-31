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
package com.terracottatech.store;

import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;

/**
 * Test to verify frs compaction by mutating repeatedly on relatively small set of records(10000).
 * Each record comprises of 20 cells with each cell carrying payload of randomly generated string of
 * size 1024.
 *
 * This test was adapted from a QA test, and is ignored because it really stress eats
 * disk space. On the other hand, I am leaving it in because you can run the test and
 * watch the data directory and see compaction happen after 3-5 minutes.
 */
public class Eugene1993Test {

  private final String STORE_NAME = "mySampleStore01";

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  private SecureRandom sr = new SecureRandom(new byte[] { 0, 1, 2, 3 });
  private long numRecords = 10000;
  private long numMutations = 500000;
  private DatasetManager datasetManager;
  private Dataset<Long> dataset;

  @Before
  public void setUp() throws IOException, StoreException {
    File path = tmpFolder.newFolder();
    datasetManager = DatasetManager.embedded().offheap("offheap", 100, MemoryUnit.GB).
      disk("disk1",
           path.toPath(),
           EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
           EmbeddedDatasetManagerBuilder.FileMode.NEW).build();
    DatasetConfiguration OFFHEAP_RESOURCE = datasetManager.datasetConfiguration()
      .offheap("offheap")
      .disk("disk1")
      .build();
    datasetManager.newDataset(STORE_NAME, Type.LONG, OFFHEAP_RESOURCE);

    dataset = datasetManager.getDataset(STORE_NAME, Type.LONG);
  }

  @After
  public void tearDown() throws StoreException {
    dataset.close();
    dataset = null;
    datasetManager.destroyDataset(STORE_NAME);
    datasetManager.close();
  }

  @Test
  @Ignore("Stress test, used if we suspect a compaction problem")
  public void test() throws Exception {
    try {

      //cell definitions
      final Random random = new Random();
      final StringCellDefinition[] cellDefinitions = new StringCellDefinition[20];
      for (int i = 0; i < cellDefinitions.length; ++i) {
        cellDefinitions[i] = CellDefinition.defineString("name-" + i);
      }

      final DatasetWriterReader<Long> access = dataset.writerReader();

      //Load phase
      AtomicLong counter = new AtomicLong();
      Collection<Future<Long>> futures = new ArrayList<>();
      int numThreads = 32;
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      System.out.println("Running load phase");
      for (int i = 0; i < numThreads; ++i) {
        futures.add(executor.submit(() -> {
          long processed = 0;
          for (; ; ) {
            //get unique record id to add
            long next = counter.incrementAndGet();
            if (next > numRecords) {
              break;
            }

            //add to data set
            Cell<?>[] cells = new Cell<?>[cellDefinitions.length];
            for (int i1 = 0; i1 < cellDefinitions.length; ++i1) {
              cells[i1] = cellDefinitions[i1].newCell(randomString(1024));
            }
            access.add(next, cells);
            if (next % 10000 == 0) {
              System.out.println("Added " + next);
            }
            ++processed;
          }
          return processed;
        }));
      }

      long totalProcessed = 0;
      for (Future<Long> f : futures) {
        totalProcessed = totalProcessed + f.get();
      }
      Assert.assertEquals(totalProcessed, numRecords);

      //Muatation iterations with each iteration performs mutation repeatedly and then
      //restart server
      int numIterations = 2;
      for (int iteration = 0; iteration < numIterations; ++iteration) {
        System.out.println("Running mutation iteration " + iteration);
        futures.clear();
        counter.set(0);
        for (int i = 0; i < numThreads; ++i) {
          futures.add(executor.submit(() -> {
            long processed = 0;
            for (; ; ) {
              //recordId to mutate
              long next = counter.incrementAndGet();
              if (next > numMutations) {
                break;
              }
              long recordId = (Math.abs(random.nextLong()) % numRecords) + 1;

              //retrieve record and mutate it
              Optional<Record<Long>> record = access.get(recordId);
              Assert.assertThat(record.isPresent(), is(true));//, "record id " + recordId + " not found");
              Cell<?>[] newCells = new Cell<?>[cellDefinitions.length];
              for (int i12 = 0; i12 < cellDefinitions.length; ++i12) {
                newCells[i12] = cellDefinitions[i12].newCell(randomString(1024));
              }
              access.update(recordId, UpdateOperation.install(newCells));
              if (next % 10000 == 0) {
                System.out.println("Mutated " + next);
              }
              ++processed;
            }
            return processed;
          }));
        }

        totalProcessed = 0;
        for (Future<Long> f : futures) {
          totalProcessed = totalProcessed + f.get();
        }
        Assert.assertEquals(totalProcessed, numMutations);

        //restart server at the end of iteration
        //control.stopAll();
        //control.startAll();
        //waitForActive(control);
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      System.out.println("Test completed. Sleep to avoid stopping servers");
      Thread.sleep(5 * 60 * 60 * 1000);
    }
  }

  String randomString(int len) {
    StringBuilder sb = new StringBuilder(len + 1);
    for (int i = 0; i < len; i++) {
      sb.append((char) sr.nextInt() & 0x7f);
    }
    return sb.toString();
  }
}



