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

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

import java.util.List;

/**
 * @author cschanck
 **/
public class DynamicallySplayingOffHeapHashMap<K, V> {

  protected final DynamicSplayableSet<HalvableOffHeapHashMap<K, V>> splayed;
  private final StorageEngine<K, V> engine;
  private final PageSource source;

  public DynamicallySplayingOffHeapHashMap(DynamicSplayableSet.HashSpreadFunc hash, PageSource source,
                                           StorageEngine<K, V> engine) {
    this(hash, source, engine, HalvableOffHeapHashMap.DEFAULT_SINGLE_MAX_CAPACITY);
  }

  public DynamicallySplayingOffHeapHashMap(DynamicSplayableSet.HashSpreadFunc hash, PageSource source,
                                           StorageEngine<K, V> engine, int maxCapacity) {
    this.source = source;
    this.engine = engine;
    this.splayed = new DynamicSplayableSet<HalvableOffHeapHashMap<K, V>>(hash,
                                                                          new HalvableOffHeapHashMap<>(source, engine,
                                                                                                        128,
                                                                                                        maxCapacity));
  }

  public V get(K k) {
    HalvableOffHeapHashMap<K, V> got = splayed.shardAt(splayed.shardIndexFor(k));
    return got.get(k);
  }

  public V put(K k, V v) {
    int index = splayed.shardIndexFor(k);
    HalvableOffHeapHashMap<K, V> got = splayed.shardAt(index);
    V ret = got.put(k, v);
    splayed.checkShard(index);
    return ret;
  }

  public V remove(K k) {
    HalvableOffHeapHashMap<K, V> got = splayed.shardAt(splayed.shardIndexFor(k));
    return got.remove(k);
  }

  public long size() {
    List<HalvableOffHeapHashMap<K, V>> sos = splayed.shards();
    long tot = 0;
    for (HalvableOffHeapHashMap<K, V> hm : sos) {
      if (hm != null) {
        tot = tot + hm.size();
      }
    }
    return tot;
  }

  public boolean isEmpty() {
    List<HalvableOffHeapHashMap<K, V>> sos = splayed.shards();
    for (HalvableOffHeapHashMap<K, V> hm : sos) {
      if (hm != null && !hm.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public void clear() {
    List<HalvableOffHeapHashMap<K, V>> sos = splayed.shards();
    for (HalvableOffHeapHashMap<K, V> hm : sos) {
      if (hm != null) {
        hm.clear();
      }
    }
    splayed.reset(new HalvableOffHeapHashMap<K, V>(source, engine, 128));
  }

  public long getReservedOverheadMemory() {
    List<HalvableOffHeapHashMap<K, V>> sos = splayed.shards();
    long tot = 0;
    for (HalvableOffHeapHashMap<K, V> hm : sos) {
      if (hm != null) {
        tot = tot + hm.getAllocatedMemory();
      }
    }
    return tot;
  }

  public long getOccupiedOverheadMemory() {
    List<HalvableOffHeapHashMap<K, V>> sos = splayed.shards();
    long tot = 0;
    for (HalvableOffHeapHashMap<K, V> hm : sos) {
      if (hm != null) {
        tot = tot + hm.getOccupiedMemory();
      }
    }
    return tot;
  }

  public long getOccupiedDataSize() {
    return engine.getOccupiedMemory();
  }

  public long getReservedDataSize() {
    return engine.getAllocatedMemory();
  }

  public boolean remove(K key, V v) {
    HalvableOffHeapHashMap<K, V> got = splayed.shardAt(splayed.shardIndexFor(key));
    return got.remove(key, v);
  }

  public void destroy() {
    List<HalvableOffHeapHashMap<K, V>> sos = splayed.shards();
    for (HalvableOffHeapHashMap<K, V> hm : sos) {
      if (hm != null) {
        hm.destroy();
      }
    }
    splayed.destroy();
  }

  public long getSize() {
    return size();
  }
}
