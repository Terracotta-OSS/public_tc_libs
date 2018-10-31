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

import com.terracottatech.sovereign.common.utils.SimpleLongObjectCache;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class SimpleLongObjectCacheTest {

  @Test
  public void testCreation() {
    SimpleLongObjectCache<Integer> cache = new SimpleLongObjectCache<Integer>(100);
    Assert.assertThat(cache.size(),is(0));
  }

  @Test
  public void testInsertNoEviction() {
    SimpleLongObjectCache<Integer> cache = new SimpleLongObjectCache<Integer>(100);
    cache.cache(100,10);
    cache.cache(101,101);
    Assert.assertThat(cache.size(), is(2));
  }

  @Test
  public void testInsertGetNoEviction() {
    SimpleLongObjectCache<Integer> cache = new SimpleLongObjectCache<Integer>(100);
    cache.cache(100,10);
    cache.cache(101,101);
    Assert.assertThat(cache.get(100L), is(10));
    Assert.assertThat(cache.get(101L), is(101));
    Assert.assertNull(cache.get(103L));
  }

  @Test
  public void testInsertWithEviction() {
    SimpleLongObjectCache<Integer> cache = new SimpleLongObjectCache<Integer>(10);
    for(int i=0;i<20;i++) {
      cache.cache(i, 2 * i);
    }
    Assert.assertThat(cache.size(),is(10));
    int cnt=0;
    for(int i=0;i<20;i++) {
      if (cache.get(i) != null) {
        cnt++;
      }
    }
    Assert.assertThat(cnt,is(10));

  }

  @Test
  public void testInsertWithEvictionIsLRU() {
    SimpleLongObjectCache<Integer> cache = new SimpleLongObjectCache<Integer>(10);
    for(int i=0;i<20;i++) {
      cache.cache(i, 2 * i);
    }
    Assert.assertThat(cache.size(),is(10));
    int cnt=0;
    for(int i=10;i<20;i++) {
      Assert.assertNotNull(cache.get(i));
    }
    cache.get(10);
    cache.cache(100,100);
    for(int i=12;i<20;i++) {
      Assert.assertNotNull(cache.get(i));
    }
    Assert.assertNotNull(cache.get(10));
    Assert.assertNotNull(cache.get(100));
  }

}
