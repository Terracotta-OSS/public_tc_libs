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

package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.impl.persistence.hybrid.SovereignHybridStorage;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.tool.RateTimer;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import io.rainfall.ObjectGenerator;
import io.rainfall.generator.LongGenerator;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author cschanck
 **/
public class TinyCellUntoDeathTest {

  final int concurrency = 1;
  final int versions = 1;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private SovereignFRSStorage getFrsStorage(File file) {
    try {
      SovereignFRSStorage store = new SovereignFRSStorage(new PersistenceRoot(file,
                                                                              PersistenceRoot.Mode.CREATE_NEW),
                                                          SovereignBufferResource.unlimited());
      store.startupMetadata().get();
      store.startupData().get();
      return store;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SovereignHybridStorage getHybridStorage(File file) {
    try {
      SovereignHybridStorage store = new SovereignHybridStorage(new PersistenceRoot(file,
                                                                                    PersistenceRoot.Mode.CREATE_NEW),
                                                                SovereignBufferResource.unlimited());
      store.startupMetadata().get();
      store.startupData().get();
      return store;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Ignore
  @Test
  public void transientTest() {
    SovereignDataset<Long> dataset = new SovereignBuilder<>(Type.LONG,
                                                            FixedTimeReference.class).offheap().limitVersionsTo(versions).concurrency(concurrency).build();
    perfTest(dataset);
  }

  @Ignore
  @Test
  public void frsTest() throws IOException {
    SovereignFRSStorage frsStore = getFrsStorage(tmp.newFolder());
    try {
      SovereignDataset<Long> dataset = new SovereignBuilder<>(Type.LONG,
                                                              FixedTimeReference.class).offheap().limitVersionsTo(versions).storage(frsStore).concurrency(
        concurrency).build();
      perfTest(dataset);
    } finally {
      frsStore.shutdown();
    }
  }

  @Ignore
  @Test
  public void hybidTest() throws IOException {
    SovereignHybridStorage hybridStore = getHybridStorage(tmp.newFolder());
    try {
      SovereignDataset<Long> dataset = new SovereignBuilder<>(Type.LONG,
                                                              FixedTimeReference.class).offheap().limitVersionsTo(versions).storage(hybridStore).concurrency(
        concurrency).build();
      perfTest(dataset);
    } finally {
      hybridStore.shutdown();
    }
  }

  public void perfTest(SovereignDataset<Long> dataset) {
    int i = 0;
    final Random rand = new Random();

    final ObjectGenerator<Long> keyGenerator = new LongGenerator();
    final CellDefinition<Integer> cellDef1 = CellDefinition.define("Int", Type.INT);

    try {
      RateTimer r = new RateTimer();
      r.start();
      while (true) {
        if (i!=0 && i % 1000000 == 0) {
          System.out.println(r.opsRateString(TimeUnit.MILLISECONDS));
        }
        Long key = keyGenerator.generate(rand.nextLong());
        Cell<?> cell1 = cellDef1.newCell(i);
         r.event(() -> {
          dataset.add(SovereignDataset.Durability.LAZY, key, cell1);
        });
        i++;
      }
    } catch (Throwable t) {
      System.out.println("*****");
      System.out.println(i);
      System.out.println("*****");
      System.err.println(t);
      throw new RuntimeException(t);
    }
  }
}
