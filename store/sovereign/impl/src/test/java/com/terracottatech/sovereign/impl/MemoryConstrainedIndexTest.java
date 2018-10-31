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

import org.junit.Test;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.resource.NamedBufferResources;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests index creation in a memory-constrained environment.
 */
public class MemoryConstrainedIndexTest {
  @Test
  public void testOutOfMemoryIndexCreate() {

    AtomicBoolean storageDepleted = new AtomicBoolean(false);
    SovereignBufferResource limitingResource = new SovereignBufferResource() {
      @Override
      public long getSize() {
        return storageDepleted.get() ? 0 : Long.MAX_VALUE;
      }

      @Override
      public long getAvailable() {
        return storageDepleted.get() ? 0 : Long.MAX_VALUE;
      }

      @Override
      public boolean reserve(int bytes) {
        return !storageDepleted.get();
      }

      @Override
      public void free(long bytes) {
      }

      @Override
      public MemoryType type() {
        return MemoryType.OFFHEAP;
      }
    };
    NamedBufferResources resources = new NamedBufferResources(Collections.singletonMap("tiny", limitingResource), "tiny");

    SovereignDataset<String> dataset = new SovereignBuilder<>(Type.STRING, SystemTimeReference.class)
        .storage(new StorageTransient(resources))
        .concurrency(2)
        .limitVersionsTo(1)
        .timeReferenceGenerator(new SystemTimeReference.Generator())
        .build();

    /*
     * Now that the dataset is created, "exhaust" off-heap storage so the index allocation fails.
     */
    storageDepleted.set(true);

    try {
      dataset.getIndexing()
          .createIndex(CellDefinition.define("someCell", Type.STRING), SovereignIndexSettings.BTREE);
      fail("Expecting SovereignExtinctionException");
    } catch (SovereignExtinctionException e) {
      assertThat(e.getCause(), instanceOf(OutOfMemoryError.class));
    }

    ((SovereignDatasetImpl)dataset).dispose();
  }
}
