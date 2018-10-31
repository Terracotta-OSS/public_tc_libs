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

package com.terracottatech.sovereign.common.splayed;

import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 3/1/2016.
 */
public class HalvableOffHeapHashMapTest {

  @Test
  public void testPutGet() throws Exception {
    UnlimitedPageSource source = new UnlimitedPageSource(new HeapBufferSource());
    SplitStorageEngine<Integer, Integer> engine = new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(),
      new IntegerStorageEngine());
    HalvableOffHeapHashMap<Integer, Integer> map = new HalvableOffHeapHashMap<>(source, engine, 128, 256);
    for (int i = 0; i < 100; i++) {
      map.put(i, i);
    }
    for (int i = 0; i < 100; i++) {
      assertThat(map.get(i), is(i));
    }
    HalvableOffHeapHashMap<Integer, Integer> m2 = map.halve((o) -> o, 0);
    for (int i = 0; i < 100; i = i + 2) {
      assertThat(m2.get(i), is(i));
      assertThat(map.get(i), nullValue());
    }
    for (int i = 1; i < 100; i = i + 2) {
      assertThat(map.get(i), is(i));
      assertThat(m2.get(i), nullValue());
    }
  }


}
