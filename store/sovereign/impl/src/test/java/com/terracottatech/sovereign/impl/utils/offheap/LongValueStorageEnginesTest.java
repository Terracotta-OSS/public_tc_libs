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

import com.terracottatech.sovereign.impl.memory.storageengines.LongValueStorageEngines;
import com.terracottatech.sovereign.impl.memory.storageengines.PrimitivePortabilityImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Did not test byte array or boolean -- they seem stupid for primary key.
 *
 * Created by cschanck on 3/23/2016.
 */
public class LongValueStorageEnginesTest {

  private UnlimitedPageSource ps;

  @Before
  public void befer() {
    this.ps = new UnlimitedPageSource(new OffHeapBufferSource());
  }

  @After
  public void tearDown() throws Exception {
    this.ps = null;
  }

  @Test
  public void testString() throws Exception {
    StorageEngine<String, Long> engine = LongValueStorageEngines.factory(PrimitivePortabilityImpl.factory(String
                                                                                                            .class), ps,
                                                                         1024,
                                                                         4096);
    test(engine, (i) -> {
      return i + "foobar" + i;
    }, 1000);
  }

  @Test
  public void testLong() throws Exception {
    StorageEngine<Long, Long> engine = LongValueStorageEngines.factory(PrimitivePortabilityImpl.LONG, ps, 1024,
                                                                       4096);
    test(engine, (i) -> {
      return (long) i;
    }, 1000);
  }

  @Test
  public void testInt() throws Exception {
    StorageEngine<Integer, Long> engine = LongValueStorageEngines.factory(PrimitivePortabilityImpl.INT, ps, 1024,
                                                                          4096);
    test(engine, (i) -> {
      return i;
    }, 1000);
  }

  @Test
  public void testChar() throws Exception {
    StorageEngine<Character, Long> engine = LongValueStorageEngines.factory(PrimitivePortabilityImpl.CHAR,
                                                                            ps,
                                                                            1024, 4096);
    test(engine, (i) -> {
      return Character.toChars(i)[0];
    }, 1000);
  }

  @Test
  public void testDouble() throws Exception {
    StorageEngine<Double, Long> engine = LongValueStorageEngines.factory(PrimitivePortabilityImpl.DOUBLE, ps,
                                                                         1024,
                                                                         4096);
    test(engine, (i) -> {
      return (double)i;
    }, 1000);
  }

  private static <K> void test(StorageEngine<K, Long> engine,
                                                  Function<Integer, K> supplier, int many) {
    TreeMap<Long, K> addrToKey = new TreeMap<>();
    HashMap<K, Long> shadow = new HashMap<>();

    for (int i = 0; i < many; i++) {
      K k = supplier.apply(i);
      long v = i * 10;
      shadow.put(k, v);
      Long addr = engine.writeMapping(k, v, 0, 0);
      addrToKey.put(addr, k);
    }

    // verify
    assertThat(shadow.size(), is(addrToKey.size()));
    for (long addr : addrToKey.keySet()) {
      K k = engine.readKey(addr, 0);
      Long v = engine.readValue(addr);
      assertThat(v.longValue(), is(shadow.get(k).longValue()));
      assertThat(k, equalTo(addrToKey.get(addr)));
    }

    // grab half to delete
    boolean delete = true;
    HashSet<Long> toDelete = new HashSet<Long>();
    for (Long addr : addrToKey.keySet()) {
      if (delete) {
        toDelete.add(addr);
      }
    }

    for (Long addr : toDelete) {
      K k = addrToKey.get(addr);
      addrToKey.remove(addr);
      engine.freeMapping(addr, 0, true);
      shadow.remove(k);
    }

    // verify
    assertThat(shadow.size(), is(addrToKey.size()));
    for (long addr : addrToKey.keySet()) {
      K k = engine.readKey(addr, 0);
      Long v = engine.readValue(addr);
      assertThat(v.longValue(), is(shadow.get(k).longValue()));
      assertThat(k, equalTo(addrToKey.get(addr)));
    }

  }

}
