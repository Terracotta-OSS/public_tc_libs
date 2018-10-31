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

package com.terracottatech.sovereign.btrees.utils;

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Collections;
import java.util.LinkedList;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author cschanck
 */
public class NIOBufferUtilsTest {

  @Test
  public void testFoo() {
    int seedSize = 64 * 1024;
    int seed = seedSize == 0 ? 0 : 32 - Integer.numberOfLeadingZeros(seedSize - 1);
    seed = 1 << seed;
    System.out.println(seed);
  }

  @Test
  public void testCopyBetweenTwoBuffers() {
    ByteBuffer b1 = ByteBuffer.allocate(100);
    b1.position(10).limit(100);
    ByteBuffer b2 = ByteBuffer.allocate(100);
    b2.position(10).limit(17);
    for (int i = 0; i < 100; i++) {
      b1.put(i, (byte) i);
    }
    NIOBufferUtils.copy(b1, 0, b2, 0, 10);
    for (int i = 0; i < 10; i++) {
      assertThat(b2.get(i), is((byte) i));
    }
    assertThat(b1.position(), is(10));
    assertThat(b2.position(), is(10));
    assertThat(b1.limit(), is(100));
    assertThat(b2.limit(), is(17));
  }

  @Test
  public void testCopyBetweenOwnBuffer() {
    ByteBuffer b1 = ByteBuffer.allocate(100);
    for (int i = 0; i < 100; i++) {
      b1.put(i, (byte) i);
    }
    NIOBufferUtils.copy(b1, 0, b1, 10, 10);
    for (int i = 0; i < 10; i++) {
      assertThat(b1.get(10 + i), is((byte) i));
    }
  }

  @Test
  public void testDualPivotSort() {
    IntBuffer buf=genRandomIntBuffer(300);
    NIOBufferUtils.dualPivotQuickSort(buf, 0, 299);
    int last=-1;
    for(int i=0;i<300;i++) {
      assertThat(buf.get(i), greaterThan(last));
      last=buf.get(i);
    }
    NIOBufferUtils.dualPivotQuickSort(buf, 0, 299);
    last=-1;
    for(int i=0;i<300;i++) {
      assertThat(buf.get(i), greaterThan(last));
      last=buf.get(i);
    }
  }

  private static IntBuffer genRandomIntBuffer(int size) {
    LinkedList<Integer> l=new LinkedList<Integer>();
    for(int i=0;i<size;i++) {
     l.add(i);
    }
    Collections.shuffle(l);
    IntBuffer buf=IntBuffer.allocate(size);
    for(int i=0;i<size;i++) {
      buf.put(i, l.remove());
    }
    return buf;
  }

  @Test
  public void testByteBufferOutputStreamSingleWrites() throws IOException {
    NIOBufferUtils.ByteBufferOutputStream stream = new NIOBufferUtils.ByteBufferOutputStream();
    for(int i=0;i<2048;i++) {
      stream.write(i);
    }
    ByteBuffer asBuf=stream.takeBuffer();
    assertThat(asBuf.remaining(), is(2048));
    for(int i=0;i<2048;i++) {
      assertThat(asBuf.get(), is((byte) (i & 0xff)));
    }
  }

  @Test
  public void testByteBufferOutputStreamBulkWrites() throws IOException {
    NIOBufferUtils.ByteBufferOutputStream stream = new NIOBufferUtils.ByteBufferOutputStream();
    for (int i = 0; i < 100; i++) {
      byte[] b = new byte[30];
      for (int j = 0; j < b.length; j++) {
        b[j] = (byte) (i & 0xff);
      }
      stream.write(b, 0, b.length);
    }
    ByteBuffer asBuf = stream.takeBuffer();
    assertThat(asBuf.remaining(), is(3000));
    assertThat(asBuf.capacity(), is(3000));
    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 30; j++) {
        assertThat(asBuf.get(),is((byte) (i & 0xff)));
      }
    }
  }
}
