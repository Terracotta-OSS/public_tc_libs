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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SortScratchPadTest {

  @Test
  public void testNaive() throws Exception {
    SortScratchPad.Naive<ByteBuffer> scratch = new SortScratchPad.Naive<>();
    ArrayList<Long> indexes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      byte[] karr = new byte[4];
      byte[] varr = new byte[10];
      ByteBuffer k = ByteBuffer.wrap(karr);
      k.putInt(0, i);
      Arrays.fill(varr, (byte) i);
      ByteBuffer v = ByteBuffer.wrap(varr);
      long addr = scratch.ingest(k, v);
      indexes.add(addr);
    }
    assertThat(scratch.count(), is(10l));
    int[] index = new int[1];
    indexes.stream().forEach((addr) -> {
      ByteBuffer k1 = scratch.fetchK(addr);
      Map.Entry<ByteBuffer, ByteBuffer> kv = scratch.fetchKV(addr);
      assertThat(k1.getInt(0), is(index[0]));
      assertThat(kv.getKey().getInt(0), is(index[0]));
      assertThat(kv.getValue().get(0), is((byte) index[0]));

      index[0]++;
    });

  }
}