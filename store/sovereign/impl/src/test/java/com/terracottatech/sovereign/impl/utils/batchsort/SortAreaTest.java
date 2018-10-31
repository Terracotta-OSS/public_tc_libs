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
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SortAreaTest {

  @Test
  public void testSimple() throws Exception {
    SortArea<ByteBuffer> area = SortArea.naive();
    assertThat(area.size(), is(0l));
    for (int i = 0; i < 10; i++) {
      byte[] karr = new byte[4];
      byte[] varr = new byte[10];
      ByteBuffer k = ByteBuffer.wrap(karr);
      k.putInt(0, i);
      Arrays.fill(varr, (byte) i);
      ByteBuffer v = ByteBuffer.wrap(varr);
      area.ingest(k, v);
    }
    assertThat(area.size(), is(10l));
    for (int i = 0; i < 10; i++) {
      ByteBuffer k1 = area.fetchK(i);
      assertThat(k1.getInt(0), is(i));
    };

    final int[] index = new int[1];
    area.asStream().forEach(value -> {
      assertThat(value.getKey().getInt(0), is(index[0]));
      assertThat(value.getValue().get(0), is((byte)index[0]));
      index[0]++;
    });

    area.swap(2,6);
    assertThat(area.fetchK(2).getInt(0), is(6));
    assertThat(area.fetchK(6).getInt(0), is(2));

  }
}