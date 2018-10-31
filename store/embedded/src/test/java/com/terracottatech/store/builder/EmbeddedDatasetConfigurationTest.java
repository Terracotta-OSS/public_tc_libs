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
package com.terracottatech.store.builder;

import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import com.terracottatech.store.indexing.IndexSettings;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.terracottatech.store.indexing.IndexSettings.BTREE;
import static org.junit.Assert.assertEquals;

public class EmbeddedDatasetConfigurationTest {
  @Test
  public void setAndGet() {
    Map<CellDefinition<?>, IndexSettings> indexes = new HashMap<>();
    CellDefinition<String> nameCellDef = CellDefinition.define("name", Type.STRING);
    indexes.put(nameCellDef, BTREE);
    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("OFFHEAP", "DISK", indexes);
    assertEquals("OFFHEAP", configuration.getOffheapResource());
    assertEquals("DISK", configuration.getDiskResource().orElse(""));
    Map<CellDefinition<?>, IndexSettings> outputIndexes = configuration.getIndexes();
    assertEquals(1, outputIndexes.size());
    assertEquals(BTREE, outputIndexes.get(nameCellDef));
  }

  @Test
  public void setAndGetWithStorageType() {
    EmbeddedDatasetConfiguration configuration =
        new EmbeddedDatasetConfiguration("OFFHEAP", "DISK",
            null, 2, null, PersistentStorageEngine.FRS);
    assertEquals("OFFHEAP", configuration.getOffheapResource());
    assertEquals("DISK", configuration.getDiskResource().orElse(""));
    assertEquals(PersistentStorageEngine.FRS, configuration.getPersistentStorageType().orElse(PersistentStorageEngine.HYBRID));
  }
}
