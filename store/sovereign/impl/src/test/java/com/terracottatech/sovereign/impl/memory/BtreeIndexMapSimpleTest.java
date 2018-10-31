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

import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.impl.SovereignAllocationResource;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.memory.BtreeIndexMap.BtreePersistentMemoryLocator;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.common.utils.SimpleFinalizer;
import com.terracottatech.store.Type;
import org.junit.Test;
import org.terracotta.offheapstore.paging.PageSource;

import static com.terracottatech.sovereign.impl.SovereignDataSetConfig.StorageType.HEAP;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by cschanck on 2/19/2016.
 */
public class BtreeIndexMapSimpleTest {


  @SuppressWarnings("unchecked")
  private ContextImpl makeContext() {
    AbstractRecordContainer<?> rc = mock(AbstractRecordContainer.class);
    when(rc.getContextFinalizer()).thenReturn(mock(SimpleFinalizer.class));
    return new ContextImpl(rc, true);
  }

  private static BtreeIndexMap<String, String> makeTree() {
    SovereignDataSetConfig<String, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.STRING,
      SystemTimeReference.class).resourceSize(HEAP, 0).versionLimit(1);
    SovereignRuntime<String> runtime = new SovereignRuntime<>(config, new CachingSequence());

    PageSource r = runtime.allocator().getNamedPageSourceAllocator(SovereignAllocationResource.Type.SortedMap);

    PageSourceLocation psl = PageSourceLocation.heap();
    BtreeIndexMap<String, String> bm = new BtreeIndexMap<>(runtime, "test", 0, String.class, psl);
    return bm;
  }

  @Test
  public void testCreate() throws Exception {
    BtreeIndexMap<String, String> bm = BtreeIndexMapSimpleTest.makeTree();
    bm.drop();
  }

  @Test
  public void testSimpleInsert() throws Exception {
    BtreeIndexMap<String, String> bm = BtreeIndexMapSimpleTest.makeTree();

    ContextImpl c = makeContext();

    for (int i = 0; i < 100; i++) {
      bm.put("key" + i, c, "key" + i, new PersistentMemoryLocator(100 + i, null));
    }

    assertThat(bm.estimateSize(), is(100l));

    bm.drop();
  }

  @Test
  public void testPutGet() throws Exception {
    BtreeIndexMap<String, String> bm = BtreeIndexMapSimpleTest.makeTree();

    ContextImpl c = makeContext();

    for (int i = 0; i < 100; i++) {
      bm.put("key" + i, c, "key" + String.format("%04d", i), new PersistentMemoryLocator(100 + i, null));
    }

    BtreePersistentMemoryLocator<?> pl = bm.get(c, "key0003");
    long ver = 103;
    while (pl.isValid()) {
      assertThat(ver, is(pl.index()));
      pl = pl.next();
      ver++;
    }

    bm.drop();

  }

  @Test
  public void testPutRemove() throws Exception {
    BtreeIndexMap<String, String> bm = BtreeIndexMapSimpleTest.makeTree();

    ContextImpl c = makeContext();

    for (int i = 0; i < 100; i++) {
      bm.put("key" + i, c, "key" + String.format("%04d", i), new PersistentMemoryLocator(100 + i, null));
    }

    for (int i = 0; i < 100; i = i + 2) {
      boolean p = bm.remove("key" + i, c, "key" + String.format("%04d", i), new PersistentMemoryLocator(100 + i, null));
      assertThat(p, is(true));
    }

    for (int i = 0; i < 100; i++) {
      String k = "key" + String.format("%04d", i);
      c = makeContext();
      PersistentMemoryLocator loc = bm.get(c, k);
      c.close();
      if (i % 2 == 1) {
        assertThat(loc.index(), is(100l + i));
      } else {
        assertThat(loc.index(), not(100l + i));
      }
    }

    bm.drop();

  }
}
