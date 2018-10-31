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
package com.terracottatech.sovereign.impl.memory.storageengines;

import com.terracottatech.sovereign.common.splayed.DynamicallySplayingOffHeapHashMap;
import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.impl.memory.storageengines.SlotPrimitivePortability.KeyAndSlot;
import com.terracottatech.store.Type;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author cschanck
 **/
public class SlotPrimitiveStore<K> {

  private final PrimitivePortability<Long> keyportable;
  private final SlotPrimitivePortability<K> valportable;
  private StorageEngine<Long, KeyAndSlot<K>> engine;
  private DynamicallySplayingOffHeapHashMap<Long, KeyAndSlot<K>> keymap;
  private AtomicLong syntheticGen = new AtomicLong(0);
  private boolean isDisposed = false;
  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
  public SlotPrimitiveStore(PageSource source, Type<K> type) {
    this.keyportable = PrimitivePortabilityImpl.LONG;
    this.valportable = SlotPrimitivePortability.factory(type.getJDKType());
//    this.engine = new OffHeapBufferStorageEngine<Long, KeyAndSlot<K>>(PointerSize.LONG, source, 1024, 16 * 1024 * 1024,
//                                                                      keyportable, valportable);

    this.engine = LongKeyStorageEngines.factory(valportable, source, 1024, 16 * 1024 * 1024);
    this.keymap = new DynamicallySplayingOffHeapHashMap<Long, KeyAndSlot<K>>(i -> MiscUtils.stirHash(i), source,
                                                                             engine);
  }

  public PrimitivePortability<K> getKPrimitive() {
    return valportable.getBasePortability();
  }

  public KeyAndSlot<K> keyAndSlot(K key, long slot) {
    return valportable.keyAndSlot(key, slot);
  }

  private void testDisposed() throws IllegalStateException {
    if (this.isDisposed) {
      throw new IllegalStateException("Attempt to use disposed lookaside key store");
    }
  }

  public int compare(long left, long right) {
    rwLock.readLock().lock();
    try {
      return compare(keymap.get(left), keymap.get(right));
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public int compare(KeyAndSlot<K> slot, KeyAndSlot<K> slot1) {
    return valportable.compare(slot, slot1);
  }

  public long add(K key, long slot) {
    rwLock.writeLock().lock();
    try {
      testDisposed();
      long ret = syntheticGen.incrementAndGet();
      keymap.put(ret, valportable.keyAndSlot(key, slot));
      return ret;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public boolean remove(long id, K key, long slot) {
    rwLock.writeLock().lock();
    try {
      return remove(id, valportable.keyAndSlot(key, slot));
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public boolean remove(long id, KeyAndSlot<K> ks) {
    rwLock.writeLock().lock();
    try {
      testDisposed();
      return keymap.remove(id, ks);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public void remove(long id) {
    rwLock.writeLock().lock();
    try {
      testDisposed();
      keymap.remove(id);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public KeyAndSlot<K> get(long id) {
    rwLock.readLock().lock();
    try {
      testDisposed();
      KeyAndSlot<K> ret = keymap.get(id);
      return ret;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public void clear() {
    rwLock.writeLock().lock();
    try {
      testDisposed();
      keymap.clear();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public void dispose() {
    rwLock.writeLock().lock();
    try {
      if (this.isDisposed) {
        return;
      }
      this.isDisposed = true;
      this.keymap.destroy();
      this.engine.destroy();
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public long getAllocatedBytes() {
    return keymap.getReservedOverheadMemory() + keymap.getReservedDataSize();
  }

  public long getOccupiedBytes() {
    return keymap.getOccupiedDataSize() + keymap.getOccupiedOverheadMemory();
  }
}
