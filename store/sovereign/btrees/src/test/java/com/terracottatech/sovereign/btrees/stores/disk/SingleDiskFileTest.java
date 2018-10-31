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

package com.terracottatech.sovereign.btrees.stores.disk;

import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.common.utils.FileUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;

public class SingleDiskFileTest {
  protected File file;

  @Before
  public void before() throws IOException {
    file = File.createTempFile("adf", "adf");
    file.delete();
  }

  @After
  public void after() throws IOException {
    if (file.exists()) {
      FileUtils.deleteRecursively(file);
    }
  }

  @Test
  public void testOpen() throws IOException {
    SimpleBlockBuffer.Factory factory = new SimpleBlockBuffer.Factory(PageSourceLocation.heap(),
      new DiskBufferProvider.Unmapped(), 1024);
    SingleDiskFile sm = new SingleDiskFile(file, PageSourceLocation.heap(), factory, 64 * 1024, 512,
      SimpleStoreStats.diskStats());
    sm.close();
  }

  @Test
  public void testWriteRead() throws IOException {
    SimpleBlockBuffer.Factory factory = new SimpleBlockBuffer.Factory(PageSourceLocation.heap(),
      new DiskBufferProvider.Unmapped(), 5192);
    SingleDiskFile sm = new SingleDiskFile(file, PageSourceLocation.heap(), factory, 64 * 1024, 4096,
      SimpleStoreStats.diskStats());

    ByteBuffer b1 = ByteBuffer.allocate(1000);
    for (int i = 0; i < b1.capacity(); i++) {
      b1.put(i, (byte) i);
    }
    int off1 = sm.allocateAndWrite(new ByteBuffer[]{b1});
    Assert.assertThat(off1, greaterThan(-1));

    ByteBuffer b2 = ByteBuffer.allocate(1000);
    sm.read(off1, b2);
    for (int i = 0; i < b1.capacity(); i++) {
      Assert.assertThat(b1.get(i), is(b2.get(i)));
    }
    sm.flush(true);

    sm.close();
  }

  @Test
  public void testSpanningWrite() throws IOException {
    SimpleBlockBuffer.Factory factory = new SimpleBlockBuffer.Factory(PageSourceLocation.heap(),
      new DiskBufferProvider.Unmapped(), 5192);
    SingleDiskFile sm = new SingleDiskFile(file, PageSourceLocation.heap(), factory, 64 * 1024, 4096,
      SimpleStoreStats.diskStats());
    try {
      ByteBuffer b1 = ByteBuffer.allocate(1000);
      for (int i = 0; i < b1.capacity(); i++) {
        b1.put(i, (byte) i);
      }
      b1.clear();
      int off1 = sm.allocateAndWrite(b1);
      Assert.assertThat(off1, greaterThan(-1));

      ByteBuffer b2 = ByteBuffer.allocate(1000);
      b2.clear();
      sm.read(off1, b2);
      for (int i = 0; i < b1.capacity(); i++) {
        Assert.assertThat(b1.get(i), Matchers.is(b2.get(i)));
      }
      sm.flush(true);
    } finally {
      sm.close();
    }
  }

  @Test
  public void testSeedAllocation() throws IOException {
    SimpleBlockBuffer.Factory factory = new SimpleBlockBuffer.Factory(PageSourceLocation.heap(),
      new DiskBufferProvider.Unmapped(), 5192);
    SingleDiskFile sm = new SingleDiskFile(file, PageSourceLocation.heap(), factory, 64 * 1024, 4096,
      SimpleStoreStats.diskStats());
    int[] allocs = new int[6];
    int jj = sm.allocated();
    for (int i = 0; i < allocs.length; i++) {
      allocs[i] = sm.allocateAndWrite(ByteBuffer.allocate(40));
    }
    sm.flush(true);
    int guard = sm.allocated();
    for (int l : allocs) {
      sm.free(l, true);
    }
    sm.flush(true);
    Assert.assertThat(sm.allocated(), Matchers.is(0));
    for (int i : allocs) {
      sm.seedAllocation(i);
    }
    sm.finishSeedingAllocations();
    Assert.assertThat(sm.allocated(), Matchers.is(guard));
    try {
      sm.seedAllocation(allocs[0] + 1);
      Assert.fail();
    } catch (Throwable e) {
    }
    sm.close();
  }
}
