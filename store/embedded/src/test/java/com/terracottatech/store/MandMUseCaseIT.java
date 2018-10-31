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

import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.PersistenceMode;
import com.terracottatech.store.definition.LongCellDefinition;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode.NEW;

/**
 * @author cschanck
 **/
@Ignore("From TDB 1355, this is mainly here to capture an ersatz M and M use pattern")
public class MandMUseCaseIT extends BasicPersistenceIT {

  public static final LongCellDefinition TIMESTAMP = defineLong("timestamp");

  @Before
  public void setUp() throws Exception {
    datasetManager = embedded().disk("disk", dataDir.newFolder().toPath(), getMode(), NEW)
      .offheap("offheap",
               256,
               MemoryUnit.MB)
      .build();
    DatasetConfigurationBuilder configBuilder = datasetManager.datasetConfiguration().disk("disk").offheap("offheap");
    configBuilder = configBuilder.index(TIMESTAMP, IndexSettings.BTREE);
    datasetManager.newDataset("dummyDataset", Type.LONG, configBuilder.build());
    dataset = datasetManager.getDataset("dummyDataset", Type.LONG);
    access = dataset.writerReader();
  }

  @Override
  protected PersistenceMode getMode() {
    return PersistenceMode.INMEMORY;
  }

  @Test
  @Ignore("As above")
  public void testMandMUseCase() {
    Random random = new Random();
    AtomicLong keyGenerator = new AtomicLong();
    Runnable reaperTask = () -> {
      try {
        long since = System.currentTimeMillis() - (30000);
        List<Long> list = access.records()
          .filter(TIMESTAMP.value().isLessThan(since))
          .limit(2)
          .map(Record::getKey)
          .collect(Collectors.toList());
        list.forEach(key -> {
          access.delete(key);
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
    Runnable fillerTask = () -> {
      try {
        byte[] ba = new byte[64];
        for (int i = 0; i < 200; i++) {
          reaperTask.run();

          long key = keyGenerator.getAndIncrement();
          random.nextBytes(ba);
          String s = new BigInteger(8 * 10 * (1 + random.nextInt(10)), random).toString(16);
          access.add(key,
                     cell("name", s),
                     cell("timestamp", System.currentTimeMillis()),
                     cell("number", random.nextInt()),
                     cell("boolean", random.nextBoolean()),
                     cell("bytes", ba),
                     cell("value", random.nextDouble()));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    };

    ScheduledExecutorService newScheduledThreadPool = Executors.newScheduledThreadPool(5);
    newScheduledThreadPool.scheduleAtFixedRate(fillerTask, 0, 1, TimeUnit.SECONDS);
    while (true) {
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("Have " + access.records().count() + " records");
    }
  }

}

