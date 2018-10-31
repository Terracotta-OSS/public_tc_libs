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

import com.terracottatech.sovereign.impl.utils.offheap.BlockStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.StorageEngine;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public class LongValueStorageEngines {
  private LongValueStorageEngines() {
  }

  public static <K> StorageEngine<K, Long> factory(PrimitivePortability<K> port, PageSource source, int initialPageSize,

                                                   int maxPageSize) {
    StorageEngine<K, Long> ret = null;
    if (port.isFixedSize()) {
      ret = new FixedKeyLongEngine<>(source, port, initialPageSize, maxPageSize);
    } else {
      ret = new PortLongEngine<>(source, port, initialPageSize, maxPageSize);
    }
    return ret;
  }

  // right now this is 12 bytes overhead; could be cut to 4
  public static class PortLongEngine<K> extends OffHeapBufferStorageEngine<K, Long> {
    public PortLongEngine(PageSource source, PrimitivePortability<K> port, int initialPageSize, int maximalPageSize) {
      super(PointerSize.LONG, source, initialPageSize, maximalPageSize, port,
            PrimitivePortabilityImpl.LONG);
    }
  }

  static class FixedKeyLongEngine<K> implements StorageEngine<K, Long> {

    private final int keySize;
    private final PrimitivePortability<K> keyPort;
    private final BlockStorageArea fixedStore;

    public FixedKeyLongEngine(PageSource ps, PrimitivePortability<K> keyPort, int initialPageSize, int maxPageSize) {
      this.keySize = keyPort.spaceNeededToEncode(null);
      this.keyPort = keyPort;
      this.fixedStore = new BlockStorageArea(ps, keySize + 8, initialPageSize, maxPageSize);
    }

    @Override
    public Long writeMapping(K k, Long aLong, int i, int i1) {
      ByteBuffer tb = ByteBuffer.allocate(keySize + 8);
      tb.putLong(aLong);
      keyPort.encode(k, tb);
      tb.clear();
      long addr = fixedStore.allocateBlock();
      fixedStore.blockAt(addr).put(tb);
      return addr;
    }

    @Override
    public void attachedMapping(long l, int i, int i1) {
      // noop
    }

    @Override
    public void freeMapping(long l, int i, boolean b) {
      fixedStore.freeBlock(l);
    }

    @Override
    public Long readValue(long l) {
      ByteBuffer tb = fixedStore.blockAt(l);
      return tb.getLong(0);
    }

    @Override
    public boolean equalsValue(Object o, long l) {
      Long val = readValue(l);
      return val != null && val.equals(o);
    }

    @Override
    public K readKey(long l, int i) {
      ByteBuffer tb = fixedStore.blockAt(l);
      tb.position(8);
      return keyPort.decode(tb.slice());
    }

    @Override
    public boolean equalsKey(Object o, long l) {
      K val = readKey(l, 0);
      return val != null && val.equals(o);
    }

    @Override
    public void clear() {
      fixedStore.clear();
    }

    @Override
    public long getAllocatedMemory() {
      return fixedStore.getTotalBlocks() * fixedStore.getBlockSize();
    }

    @Override
    public long getOccupiedMemory() {
      return (fixedStore.getTotalBlocks() - fixedStore.getFreeBlocks()) * fixedStore.getBlockSize();
    }

    @Override
    public long getVitalMemory() {
      return getAllocatedMemory();
    }

    @Override
    public long getDataSize() {
      return getOccupiedMemory();
    }

    @Override
    public void invalidateCache() {
      // noop
    }

    @Override
    public void bind(Owner owner) {
      // noop
    }

    @Override
    public void destroy() {
      fixedStore.dispose();
    }

    @Override
    public boolean shrink() {
      return false;
    }
  }

}
