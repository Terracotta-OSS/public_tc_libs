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

package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.IntCellDefinition;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.definition.CellDefinition.defineInt;

/**
 * Superclass for test suites verifying the correctness of queries using indices.
 */
abstract class AbstractIndexUsageTest {

  SovereignDataset<String> dataset = null;

  static final IntCellDefinition C1 = defineInt("test1");
  static final IntCellDefinition C2 = defineInt("test2");
  static final IntCellDefinition C3 = defineInt("test3");

  @Before
  public void before() throws Exception {
    dataset = new SovereignBuilder<>(Type.STRING, FixedTimeReference.class).offheap(128 * 1024 * 1024).limitVersionsTo(1).build();
    loadCells(10);
    dataset.getIndexing().createIndex(C1, SovereignIndexSettings.BTREE).call();
    dataset.getIndexing().createIndex(C2, SovereignIndexSettings.BTREE).call();
  }

  private void loadCells(int numCells) {
    for (int i = 0; i < numCells; i++) {
      dataset.add(IMMEDIATE, "key_" + i, C1.newCell(i), C2.newCell(i + numCells), C3.newCell(i + 2 * numCells));
    }
  }

  @After
  public void after() throws IOException {
    dataset.getStorage().destroyDataSet(dataset.getUUID());
    dataset = null;
    System.gc();
  }
}
