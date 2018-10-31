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

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

import java.nio.IntBuffer;

/**
 * @author cschanck
 **/
public class HalvableOffHeapHashMap<K, V> extends OffHeapHashMap<K, V>
  implements DynamicSplayableSet.SplayedObject<HalvableOffHeapHashMap<K, V>> {
  // WAG. Singleton will hold 16 million or so.
  public static final int DEFAULT_SINGLE_MAX_CAPACITY = Integer.getInteger("sovereign.hashindexmap.maxcap",
                                                                           6 * 1000 * 1000).intValue();

  private final int maxCapacity;

  public HalvableOffHeapHashMap(PageSource source, StorageEngine<K, V> engine, int capacity) {
    this(source, engine, capacity, DEFAULT_SINGLE_MAX_CAPACITY);
  }

  public HalvableOffHeapHashMap(PageSource source, StorageEngine<K, V> engine, int capacity, int maxCapacity) {
    super(source, new OwnerInterceptorStorageEngine<K, V>(engine), capacity);
    this.maxCapacity = maxCapacity;
  }

  @Override
  public HalvableOffHeapHashMap<K, V> object() {
    return this;
  }

  @FindbugsSuppressWarnings("VO_VOLATILE_INCREMENT")
  public HalvableOffHeapHashMap<K, V> halve(DynamicSplayableSet.HashSpreadFunc hashFunc, int bitPos) {
    int mask = 1 << bitPos;

    @SuppressWarnings("unchecked")
    HalvableOffHeapHashMap<K, V> newbie = new HalvableOffHeapHashMap<>(tableSource, (StorageEngine<K, V>) this.storageEngine,
                                                                       hashtable.capacity() / ENTRY_SIZE, maxCapacity);
    IntBuffer tbl = (IntBuffer) hashtable.duplicate().clear();
    newbie.reprobeLimit = reprobeLimit;

    while (tbl.hasRemaining()) {
      IntBuffer entry = (IntBuffer) tbl.slice().limit(ENTRY_SIZE);
      if (isPresent(entry)) {
        int hcode = entry.get(KEY_HASHCODE);
        int h = hashFunc.hash(hcode);
        if ((h & mask) == 0) {

          // add to newbie.
          newbie.installEntry(hcode, entry);

          // clear locally
          entry.slice().put(STATUS, STATUS_REMOVED);
          ++this.modCount;
          ++this.removedSlots;
          --this.size;
        }
      }
      tbl.position(tbl.position() + ENTRY_SIZE);
    }
    newbie.shrinkTable();
    shrinkTable();
    return newbie;
  }

  @FindbugsSuppressWarnings("VO_VOLATILE_INCREMENT")
  private void installEntry(int hash, IntBuffer newEntry) {

    int start = indexFor(spread(hash));
    hashtable.position(start);

    int limit = reprobeLimit;
    // this can make a bunhc of assumptions, mainly that the keys should
    // never collide, and there will be room, and that it will always find a
    // spot to place it within the reprobe level. This is all due to the entries
    // coming from another stable hashmap.
    for (int i = 0; i < limit; i++) {
      if (!hashtable.hasRemaining()) {
        hashtable.rewind();
      }
      IntBuffer entry = (IntBuffer) hashtable.slice().limit(ENTRY_SIZE);
      if (isAvailable(entry)) {
        entry.slice().put(newEntry.slice());
        ++this.size;
        return;
      } else {
        hashtable.position(hashtable.position() + ENTRY_SIZE);
      }
    }
    throw new IllegalStateException("reinstall probe failure");
  }

  @Override
  public boolean isOversized() {
    boolean ret = size() >= maxCapacity;
    if (ret) {
      int n = 0;
    }
    return ret;
  }

  @Override
  public long getOccupiedMemory() {
    return super.getUsedSlotCount() * ENTRY_SIZE * Integer.BYTES;
  }

  @Override
  public long getAllocatedMemory() {
    return super.getTableCapacity() * ENTRY_SIZE * Integer.BYTES;
  }
}
