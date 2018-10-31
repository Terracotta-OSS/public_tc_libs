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
package com.terracottatech.sovereign.impl.utils.batchsort;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class AbstractSortTest {

  static Comparator<ByteBuffer> LONG_COMP = new Comparator<ByteBuffer>() {
    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
      return Long.compare(o1.getLong(0), o2.getLong(0));
    }
  };

  public abstract SortArea<ByteBuffer> makeArea() throws IOException;

  public abstract void sort(SortArea<ByteBuffer> a, Comparator<ByteBuffer> comp) throws IOException;

  public final long numberOfElements() {
    return 100000;
  }

  @Test
  public void testSort() throws IOException {
    SortArea<ByteBuffer> area = makeArea();
    Random r = new Random(0);
    try {
      System.out.println("Populating: "+numberOfElements());
      populate(area, r);
      System.out.println("Sorting...");
      long st = System.nanoTime();
      sort(area, LONG_COMP);
      long tookNS = System.nanoTime() - st;
      System.out.println(TimeUnit.MILLISECONDS.convert(tookNS, TimeUnit.NANOSECONDS) + "ms");
      System.out.println("Verifying...");
      verify(area);
    } finally {
      makeArea().dispose();
    }
  }

  private void verify(SortArea<ByteBuffer> area) {
    assertThat(area.size(), is(numberOfElements()));
    long[] last = new long[] { Long.MIN_VALUE };
    long[] cnt=new long[1];
    area.asStream().forEach((v) -> {
      long val = v.getKey().getLong(0);
      assertTrue(val >= last[0]);
      last[0] = val;
      cnt[0]++;
    });
    System.out.println("Verified: "+cnt[0]);
  }

  private void populate(SortArea<ByteBuffer> area, Random r) throws IOException {
    for (long l = 0; l < numberOfElements(); l++) {
      long kval = r.nextLong();
      ByteBuffer k = ByteBuffer.allocate(8);
      k.putLong(0, kval);
      ByteBuffer v = ByteBuffer.allocate(100);
      fill(v, (byte) kval);
      area.ingest(k, v);
    }
  }

  private void fill(ByteBuffer v, byte val) {
    while (v.hasRemaining()) {
      v.put(val);
    }
    v.clear();
  }

}
