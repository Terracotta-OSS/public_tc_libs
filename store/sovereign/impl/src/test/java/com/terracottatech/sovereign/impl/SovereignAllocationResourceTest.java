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
import org.junit.Assert;
import org.junit.Test;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.definition.CellDefinition.defineString;

public class SovereignAllocationResourceTest {
  @Test
  public void testExtinctionMessage() throws Exception {
    StorageTransient store = new StorageTransient(SovereignBufferResource.limited(SovereignBufferResource.MemoryType.OFFHEAP,
                                                                                  1024 * 1024));
    try {
      SovereignDataset<Long> ds = new SovereignBuilder<Long, SystemTimeReference>(Type.LONG, SystemTimeReference.class).concurrency(
        1).storage(store).build();

      try {
        for (int i = 0; i < 10000; i++) {
          ds.add(IMMEDIATE, (long) i, defineString("foo").newCell("such a happy time"));
        }
        Assert.fail();
      } catch (SovereignExtinctionException e) {
        Assert.assertTrue(e.getMessage().contains("OutOfMemory"));
        Assert.assertTrue(e.getMessage().contains("of 1048576 bytes allocated]"));
      }
    } finally {
      store.shutdown();
    }
  }
}