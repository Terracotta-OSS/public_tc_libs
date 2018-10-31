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

package com.terracottatech.sovereign.btrees.stores.memory;

import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.StoreOutOfSpaceException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import static com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation.heap;
import static org.hamcrest.core.Is.is;

public class SinglePagedMemorySpaceTest {

  public SinglePagedMemorySpace getSpace() throws StoreOutOfSpaceException {
    return new SinglePagedMemorySpace(heap(), 64 * 1024 * 1024, 4096, SimpleStoreStats.memoryStats());
  }

  @Test
  public void testOpen() throws IOException {
    SinglePagedMemorySpace lob = getSpace();
  }

  @Test
  public void testReadWriteData() throws IOException {
    SinglePagedMemorySpace lob = getSpace();

    ByteBuffer b1 = ByteBuffer.allocate(1024);
    b1.clear();
    fill(b1, 1);
    int got = lob.append(b1);

    b1.clear();
    fill(b1, 0);
    lob.read(got, b1);
    IntBuffer ib = b1.asIntBuffer().slice();
    for (int i = 0; i < ib.capacity(); i++) {
      Assert.assertThat(ib.get(i), is(1));
    }
  }

  private void fill(ByteBuffer b1, int i) {
    IntBuffer tmp = b1.duplicate().asIntBuffer();
    while (tmp.hasRemaining()) {
      tmp.put(i);
    }
  }

  @Test
  public void testReclaiming() throws IOException {
    SinglePagedMemorySpace lob = getSpace();
    int old = lob.append(ByteBuffer.allocate(10));
    lob.free(old);
    int old2 = lob.append(ByteBuffer.allocate(10));
    Assert.assertThat(old, is(old2));
  }

}
