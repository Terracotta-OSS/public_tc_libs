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
import java.util.Comparator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SortIndexTest {
  static Comparator<ByteBuffer> intComp = new Comparator<ByteBuffer>() {
    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
      return Integer.compare(o1.getInt(0), o2.getInt(0));
    }
  };

  @Test
  public void testNaive() throws Exception {
    SortIndex.Naive si = new SortIndex.Naive();
    si.add(10);
    si.add(8);
    si.add(12);
    si.add(6);
    si.add(14);

    assertThat(si.size(), is(5l));
    assertThat(si.addressOf(0), is(10l));
    assertThat(si.addressOf(1), is(8l));
    assertThat(si.addressOf(2), is(12l));
    assertThat(si.addressOf(3), is(6l));
    assertThat(si.addressOf(4), is(14l));

    final int[] index = new int[1];
    si.inOrderStream().forEach(value -> {
      long shouldBe = si.addressOf(index[0]);
      index[0]++;
      assertThat(value, is(shouldBe));
    });

    si.swap(1,3);
    assertThat(si.addressOf(0), is(10l));
    assertThat(si.addressOf(1), is(6l));
    assertThat(si.addressOf(2), is(12l));
    assertThat(si.addressOf(3), is(8l));
    assertThat(si.addressOf(4), is(14l));

  }
}