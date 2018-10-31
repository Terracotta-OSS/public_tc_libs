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

import com.terracottatech.sovereign.impl.memory.storageengines.SlotPrimitivePortability.KeyAndSlot;
import com.terracottatech.sovereign.impl.memory.storageengines.SlotPrimitiveStore;
import com.terracottatech.store.Type;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 2/19/2016.
 */
public class SlotPrimitiveStoreTest {

  @Test
  public void testSimpleInit() throws Exception {
    SlotPrimitiveStore<Boolean> s = new SlotPrimitiveStore<Boolean>(new UnlimitedPageSource(new HeapBufferSource()),
                                                                    Type.BOOL);
    s.dispose();
  }

  @Test
  public void testBoolean() throws Exception {
    SlotPrimitiveStore<Boolean> s = new SlotPrimitiveStore<Boolean>(new UnlimitedPageSource(new HeapBufferSource()),
      Type.BOOL);

    Function<Integer, Boolean> supplier = (i) -> {
      return (i & 0b1) == 0;
    };

    innerTest(s, supplier);
    s.dispose();
  }

  @Test
  public void testChar() throws Exception {
    SlotPrimitiveStore<Character> s = new SlotPrimitiveStore<>(new UnlimitedPageSource(new HeapBufferSource()),
      Type.CHAR);

    Function<Integer, Character> supplier = (i) -> {
      String chars = "abscdefgsdlfslmsdmgbl;kasdff";
      return chars.charAt(i % chars.length());
    };

    innerTest(s, supplier);
    s.dispose();
  }

  @Test
  public void testInt() throws Exception {
    SlotPrimitiveStore<Integer> s = new SlotPrimitiveStore<>(new UnlimitedPageSource(new HeapBufferSource()), Type.INT);

    Function<Integer, Integer> supplier = (i) -> {
      return i * 3;
    };

    innerTest(s, supplier);
    s.dispose();
  }

  @Test
  public void testLong() throws Exception {
    SlotPrimitiveStore<Long> s = new SlotPrimitiveStore<>(new UnlimitedPageSource(new HeapBufferSource()), Type.LONG);

    Function<Integer, Long> supplier = (i) -> {
      return (long) (i * 3);
    };

    innerTest(s, supplier);
    s.dispose();
  }

  @Test
  public void testDouble() throws Exception {
    SlotPrimitiveStore<Double> s = new SlotPrimitiveStore<>(new UnlimitedPageSource(new HeapBufferSource()),
      Type.DOUBLE);

    Function<Integer, Double> supplier = (i) -> {
      return ((double) (i * 3)) / 2d;
    };

    innerTest(s, supplier);
    s.dispose();
  }

  @Test
  public void testString() throws Exception {
    SlotPrimitiveStore<String> s = new SlotPrimitiveStore<>(new UnlimitedPageSource(new HeapBufferSource()),
      Type.STRING);

    Random r = new Random(0);
    Function<Integer, String> supplier = (i) -> {
      StringBuffer sb = new StringBuffer();
      int len = 10 + r.nextInt(50);
      for (int j = 0; j < len; j++) {
        char c = (char) (r.nextInt(26) + 'a');
        sb.append(c);
      }
      return sb.toString();
    };

    innerTest(s, supplier);
    s.dispose();
  }

  @Test
  public void testTAB6735() throws UnsupportedEncodingException {
    SlotPrimitiveStore<String> s = new SlotPrimitiveStore<>(new UnlimitedPageSource(new HeapBufferSource()),
      Type.STRING);
    byte[] badchars = new byte[] { (byte) -20, (byte) -118, (byte) -127, (byte) -24, (byte) -96, (byte) -66 };
    String str = new String(badchars, "UTF8");
    s.add(str, 10);
  }

  @Test
  public void testByteArray() throws Exception {
    SlotPrimitiveStore<byte[]> s = new SlotPrimitiveStore<>(new UnlimitedPageSource(new HeapBufferSource()),
      Type.BYTES);

    Random r = new Random(0);
    Function<Integer, byte[]> supplier = (i) -> {
      StringBuffer sb = new StringBuffer();
      int len = 10 + r.nextInt(50);
      byte[] b = new byte[len];
      for (int j = 0; j < len; j++) {
        b[j] = (byte) (r.nextInt(127));
      }
      return b;
    };

    innerTest(s, supplier);
    s.dispose();
  }

  private <T> void innerTest(SlotPrimitiveStore<T> s, Function<Integer, T> supplier) {
    Map<Long, T> slotToValue = new HashMap<>();
    List<Long> addrs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      T val = supplier.apply(i);
      long addr = s.add(val, i);
      slotToValue.put((long) i, val);
      addrs.add(addr);
    }
    for (int i = 0; i < 100; i = i + 4) {
      T val = slotToValue.get((long) i);
      s.remove(addrs.get(i), val, i);
    }
    for (int i = 0; i < 100; i++) {
      KeyAndSlot<T> ks = s.get(addrs.get(i));
      if (i % 4 == 0) {
        assertThat(ks, CoreMatchers.nullValue());
      } else {
        assertThat(ks.getSlot(), is((long) i));
        assertThat(ks.getKey(), is(slotToValue.get((long) i)));
      }
    }
  }

}
