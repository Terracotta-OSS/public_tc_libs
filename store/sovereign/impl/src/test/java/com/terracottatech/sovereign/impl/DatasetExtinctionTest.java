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
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import com.terracottatech.sovereign.impl.persistence.StorageTransient;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author cschanck
 **/
public class DatasetExtinctionTest {
  @Test
  public void testTooSmallToStart() throws Exception {
    SovereignBufferResource r = new SovereignBufferResource() {
      @Override
      public long getSize() {
        return 0;
      }

      @Override
      public long getAvailable() {
        return 0;
      }

      @Override
      public boolean reserve(int bytes) {
        return false;
      }

      @Override
      public void free(long bytes) {
      }

      @Override
      public MemoryType type() {
        return MemoryType.HEAP;
      }

      @Override
      public String toString() {
        return defaultToString();
      }
    };

    StorageTransient s = new StorageTransient(r);
    try {
      SovereignDataset<String> ds = new SovereignBuilder<>(Type.STRING, SystemTimeReference.class).heap()
        .storage(s)
        .alias("test")
        .build();
      Assert.fail();
    } catch (SovereignExtinctionException e) {
    }
    Optional<SovereignDatasetImpl<?>> p = s.getManagedDatasets().stream().filter((ds) -> {
      return ds.getAlias().equals("test");
    }).findFirst();
    assertThat(p.isPresent(), is(false));
  }

  @Test
  public void testTooSmall() throws Exception {
    SovereignBufferResource r = new SovereignBufferResource() {
      private final long capacity = 128 * 1024;
      long max = capacity;

      @Override
      public long getSize() {
        return capacity;
      }

      @Override
      public long getAvailable() {
        return max;
      }

      @Override
      public boolean reserve(int bytes) {
        if (max > 0 && max >= bytes) {
          max = max - bytes;
          return true;
        }
        return false;
      }

      @Override
      public void free(long bytes) {
        max = max + bytes;
      }

      @Override
      public MemoryType type() {
        return MemoryType.HEAP;
      }

      @Override
      public String toString() {
        return defaultToString();
      }
    };

    StorageTransient s = new StorageTransient(r);
    SovereignDataset<String> ds = new SovereignBuilder<>(Type.STRING, SystemTimeReference.class).heap().storage(s).alias(
      "test").build();
    StringCellDefinition celldef = CellDefinition.defineString("foo");
    int i = 0;
    for (; i < 100000; i++) {
      try {
        ds.add(SovereignDataset.Durability.LAZY, "key" + i, celldef.newCell("valueofnewcell" + i));
      } catch (SovereignExtinctionException e) {
        break;
      }
    }
    assertThat(i, lessThan(100000));
    assertThat(ds.isDisposed(), is(true));
    assertThat(ds.getExtinctionException(), notNullValue());
    Optional<SovereignDatasetImpl<?>> p = s.getManagedDatasets().stream().filter((d) -> {
      return d.getAlias().equals("test");
    }).findFirst();
    assertThat(p.isPresent(), is(true));
    SovereignDatasetImpl<?> ds1 = p.get();
    assertThat(ds1.isDisposed(), is(true));
    assertThat(ds1.getExtinctionException(), notNullValue());
    s.destroyDataSet(ds1.getUUID());
    Optional<SovereignDatasetImpl<?>> p2 = s.getManagedDatasets().stream().filter((d) -> {
      return d.getAlias().equals("test");
    }).findFirst();
    assertThat(p2.isPresent(), is(false));

  }
}
