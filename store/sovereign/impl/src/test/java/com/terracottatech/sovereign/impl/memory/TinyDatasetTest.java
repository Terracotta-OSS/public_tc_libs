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
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author cschanck
 **/
public class TinyDatasetTest {

  @Test
  public void testTinyWithIndex() throws Exception {
    SovereignDataset<String> dataset = new SovereignBuilder<>(Type.STRING,
                                                              FixedTimeReference.class).offheap(10 * 1024 * 1024).limitVersionsTo(
      1).concurrency(1).build();
    try {
      CellDefinition<Integer> oneDef = CellDefinition.define("one", Type.INT);
      CellDefinition<Integer> twoDef = CellDefinition.define("two", Type.INT);
      for (int i = 0; i < 10; i++) {
        dataset.add(SovereignDataset.Durability.IMMEDIATE, "k" + i, oneDef.newCell(i), twoDef.newCell(i * 2));
      }
      dataset.getIndexing().createIndex(oneDef, SovereignIndexSettings.BTREE).call();
    } catch (Throwable t) {
      Assert.fail(t.getMessage());
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }

  }
}
