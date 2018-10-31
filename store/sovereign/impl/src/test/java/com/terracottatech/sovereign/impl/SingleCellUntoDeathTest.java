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
import com.terracottatech.sovereign.impl.persistence.base.AbstractRestartabilityBasedStorage;
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.sovereign.impl.persistence.hybrid.SovereignHybridStorage;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import io.rainfall.ObjectGenerator;
import io.rainfall.generator.ByteArrayGenerator;
import io.rainfall.generator.LongGenerator;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Random;

/**
 * @author cschanck
 **/
public class SingleCellUntoDeathTest {

  final int payloadSize = 8;
  final int reportInterval = 1000000;
  final int concurrency = 1;
  final int versions = 1;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private <T extends AbstractRestartabilityBasedStorage> T getStorage(T store) throws Exception {
    store.startupMetadata().get();
    store.startupData().get();
    return store;
  }

  private PersistenceRoot getRoot() throws IOException {
    return new PersistenceRoot(tmp.newFolder(), PersistenceRoot.Mode.CREATE_NEW);
  }

  private SovereignFRSStorage getFrsStorage() throws Exception {
    return getStorage(new SovereignFRSStorage(getRoot(), SovereignBufferResource.unlimited()));
  }

  private SovereignHybridStorage getHybridStorage() throws Exception {
    return getStorage(new SovereignHybridStorage(getRoot(), SovereignBufferResource.unlimited()));
  }

  @Ignore
  @Test
  public void transientTest() {
    SovereignDataset<Long> dataset = new SovereignBuilder<>(Type.LONG,
                                                            FixedTimeReference.class).offheap().limitVersionsTo(
      versions).concurrency(concurrency).build();
    perfTest((SovereignDatasetImpl<Long>) dataset);
  }

  @Ignore
  @Test
  public void frsTest() throws Exception {
    SovereignFRSStorage frsStore = getFrsStorage();
    try {
      SovereignDataset<Long> dataset = new SovereignBuilder<>(Type.LONG,
                                                              FixedTimeReference.class).offheap().limitVersionsTo(
        versions).storage(frsStore).concurrency(concurrency).build();
      perfTest((SovereignDatasetImpl<Long>) dataset);
    } finally {
      frsStore.shutdown();
    }
  }

  @Ignore
  @Test
  public void hybridTest() throws Exception {
    SovereignHybridStorage hybridStore = getHybridStorage();
    try {
      SovereignDataset<Long> dataset = new SovereignBuilder<>(Type.LONG,
                                                              FixedTimeReference.class).offheap().limitVersionsTo(
        versions).storage(hybridStore).concurrency(concurrency).build();
      perfTest((SovereignDatasetImpl<Long>) dataset);
    } finally {
      hybridStore.shutdown();
    }
  }

  public void perfTest(SovereignDatasetImpl<Long> dataset) {
    int i = 0;
    final Random rand = new Random();

    final ObjectGenerator<Long> keyGenerator = new LongGenerator();
    final ObjectGenerator<byte[]> valueGenerator = new ByteArrayGenerator(payloadSize);
    final CellDefinition<byte[]> cellDef = CellDefinition.define("Bytes", Type.BYTES);

    try {
      while (true) {
        i++;
        if (i % reportInterval == 0) {
          System.out.println(i);
          System.out.println(dataset.getStatsDump());
        }
        Long key = keyGenerator.generate(rand.nextLong());
        byte[] value = valueGenerator.generate(rand.nextLong());
        Cell<?> cell = cellDef.newCell(value);
        dataset.add(SovereignDataset.Durability.LAZY, key, cell);
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
