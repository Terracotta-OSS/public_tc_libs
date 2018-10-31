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

import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.impl.memory.storageengines.LongValueStorageEngines;
import com.terracottatech.sovereign.impl.memory.storageengines.PrimitivePortabilityImpl;
import com.terracottatech.sovereign.impl.model.SovereignPrimaryMap;
import com.terracottatech.sovereign.impl.model.SovereignShardObject;
import com.terracottatech.sovereign.common.splayed.DynamicallySplayingOffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author mscott
 */
public class HashIndexMap<K extends Comparable<K>> implements SovereignPrimaryMap<K>, SovereignShardObject {
  private final SovereignRuntime<K> runtime;
  private final int shardIndex;
  private StorageEngine<K, Long> engine;
  private DynamicallySplayingOffHeapHashMap<K, Long> backing;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private boolean isDropped = false;
  private LongAdder accessCount=new LongAdder();

  HashIndexMap(SovereignRuntime<K> runtime, int shardIndex, Class<K> type, PageSource pageSource) {
    this.runtime = runtime;
    this.shardIndex = shardIndex;
    this.engine = LongValueStorageEngines.factory(PrimitivePortabilityImpl.factory(type), pageSource, 1024,
                                                  16 * 1024 * 1024);
    this.backing = new DynamicallySplayingOffHeapHashMap<>(i -> MiscUtils.stirHash(i), pageSource, engine);
  }

  long getCapacity() {
    rwLock.readLock().lock();
    try {
      this.testDropped();
      return getAllocatedStorageSize();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  long getUsed() {
    rwLock.readLock().lock();
    try {
      this.testDropped();
      return backing.getOccupiedDataSize() + backing.getOccupiedOverheadMemory();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  long getReserved() {
    return getCapacity();
  }

  @Override
  public PersistentMemoryLocator get(ContextImpl c, K key) {
    rwLock.readLock().lock();
    try {
      this.testDropped();
      accessCount.increment();
      Long val = backing.get(key);
      if (val == null) {
        return PersistentMemoryLocator.INVALID;
      }
      return new PersistentMemoryLocator(val, null);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void reinstall(Object key, PersistentMemoryLocator loc) {
    rwLock.writeLock().lock();
    try {
      this.testDropped();
      @SuppressWarnings("unchecked")
      K k = (K) key;
      backing.put(k, loc.index());
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public PersistentMemoryLocator put(ContextImpl c, K key, PersistentMemoryLocator loc) {
    rwLock.writeLock().lock();
    try {
      this.testDropped();
      Long last = backing.put(key, loc.index());
      if (last != null) {
        return new PersistentMemoryLocator(last, null);
      } else {
        return PersistentMemoryLocator.INVALID;
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public boolean remove(ContextImpl c, K key, PersistentMemoryLocator loc) {
    rwLock.writeLock().lock();
    try {
      this.testDropped();
      return backing.remove(key, loc.index());
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public void drop() {
    if (this.isDropped) {
      return;
    }
    rwLock.writeLock().lock();
    try {
      if (this.isDropped) {
        return;
      }
      this.isDropped = true;
      this.backing.destroy();
      this.backing = null;
      this.engine.destroy();
      this.engine = null;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private void testDropped() throws IllegalStateException {
    if (this.isDropped) {
      throw new IllegalStateException("Attempt to use dropped map");
    }
  }

  @Override
  public long estimateSize() {
    rwLock.readLock().lock();
    try {
      this.testDropped();
      return backing.size();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("HashIndexMap{");
    sb.append("isDropped=").append(isDropped);

    DynamicallySplayingOffHeapHashMap<K, Long> backingMap = backing;
    if (backingMap == null) {
      sb.append(", backing=null");
    } else {
      // use both calls here rather than the combined above to avoid locking.
      sb.append(", allocated=").append(getAllocatedStorageSize()).append(", " + "size=").append(backingMap.getSize());
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public SovereignRuntime<K> runtime() {
    return runtime;
  }

  @Override
  public long getOccupiedStorageSize() {
    try {
      return backing.getOccupiedDataSize() + backing.getOccupiedOverheadMemory();
    } catch(Throwable t) {
    }
    return 0l;
  }

  @Override
  public long getAllocatedStorageSize() {
    try {
      return backing.getReservedDataSize() + backing.getReservedOverheadMemory();
    } catch(Throwable t) {
    }
    return 0l;
  }

  @Override
  public long statAccessCount() {
    return accessCount.longValue();
  }

  @Override
  public int getShardIndex() {
    return shardIndex;
  }
}
