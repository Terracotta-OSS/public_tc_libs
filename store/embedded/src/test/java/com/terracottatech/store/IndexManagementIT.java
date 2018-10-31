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

import com.terracottatech.store.async.Operation;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.manager.DatasetManager;
import org.junit.Test;

import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.manager.DatasetManager.embedded;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class IndexManagementIT {

  @Test
  public void testIndexLifecycle() throws Exception {
    try (DatasetManager datasetManager = embedded().offheap("offheap", 10, MemoryUnit.MB).build()) {
      IntCellDefinition foo = defineInt("foo");

      assertThat(datasetManager.newDataset("test", Type.LONG,
          datasetManager.datasetConfiguration().offheap("offheap").build()), is(true));

      try (Dataset<Long> dataset = datasetManager.getDataset("test", Type.LONG)) {
        DatasetWriterReader<Long> access = dataset.writerReader();
        Indexing indexing = dataset.getIndexing();
        Operation<Index<Integer>> indexCreation = indexing.createIndex(foo, IndexSettings.BTREE);
        access.add(0L, foo.newCell(1));
        access.add(1L, foo.newCell(2));

        Index<Integer> index = indexCreation.get();
        assertThat(indexing.getLiveIndexes(), hasSize(1));

        assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(1)).count(), is(2L));
        access.add(2L, foo.newCell(2));
        assertThat(access.records().filter(foo.value().isGreaterThanOrEqualTo(1)).count(), is(3L));

        indexing.destroyIndex(index);

        assertThat(indexing.getLiveIndexes(), empty());
        assertThat(indexing.getAllIndexes(), empty());
      }
    }
  }
}
