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

package com.terracottatech.sovereign.impl.utils.offheap;

import org.junit.Test;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import java.util.TreeMap;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 3/22/2016.
 */
public class BlockStorageAreaTest {

  @Test
  public void testCreate() throws Exception {
    PageSource ps = new UnlimitedPageSource(new OffHeapBufferSource());
    BlockStorageArea bs = new BlockStorageArea( ps, 16, 512, 1024);
    assertThat(bs.getFreeBlocks(), is(0));
    assertThat(bs.getBlockSize(), is(16));
    assertThat(bs.getTotalBlocks(), is(0));
  }

  @Test
  public void testCRUD() throws Exception {
    PageSource ps = new UnlimitedPageSource(new OffHeapBufferSource());
    BlockStorageArea bs = new BlockStorageArea( ps, 16, 512, 1024);

    TreeMap<Integer, Long> m = new TreeMap<>();
    for (int i = 0; i < 100; i++) {
      long b = bs.allocateBlock();
      bs.blockAt(b).putInt(0, i);
      m.put(i, b);
    }
    assertThat(bs.getFreeBlocks(), is(60));
    assertThat(bs.getTotalBlocks(), is(160));
    for (int i : m.keySet()) {
      assertThat(bs.blockAt(m.get(i)).getInt(0), is(i));
    }
    for (int i : m.keySet()) {
      if ((i % 2) == 0) {
        bs.freeBlock(m.get(i));
      }
    }
    assertThat(bs.getFreeBlocks(), is(bs.getTotalBlocks() - 50));
    for (int i : m.keySet()) {
      if ((i % 2) == 0) {
        long b = bs.allocateBlock();
        bs.blockAt(b).putInt(0, i * 2);
        m.put(i, b);
      }
    }
    assertThat(bs.getFreeBlocks(), is(60));
    assertThat(bs.getTotalBlocks(), is(160));
//    for (int i : m.keySet()) {
//      if ((i % 2) == 0) {
//        assertThat(mb.blockAt(m.get(i)).getInt(0), is(i * 2));
//      } else {
//        assertThat(mb.blockAt(m.get(i)).getInt(0), is(i));
//      }
//    }

  }
}
