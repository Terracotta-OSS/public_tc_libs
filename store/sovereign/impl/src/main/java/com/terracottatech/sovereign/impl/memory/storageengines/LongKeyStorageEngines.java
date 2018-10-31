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
public class LongKeyStorageEngines {
  private LongKeyStorageEngines() {
  }

  public static <V> StorageEngine<Long, V> factory(PrimitivePortability<V> port, PageSource source, int initialPageSize,

                                                   int maxPageSize) {
    StorageEngine<Long, V> ret = null;
    if (port.isFixedSize()) {
      ret = new FixedValLongEngine<>(source, port, initialPageSize, maxPageSize);
    } else {
      ret = new LongPortEngine<>(source, port, initialPageSize, maxPageSize);
    }
    return ret;
  }

  // right now this is 12 bytes overhead; could be cut to 4
  public static class LongPortEngine<V> extends OffHeapBufferStorageEngine<Long, V> {
    public LongPortEngine(PageSource source, PrimitivePortability<V> port, int initialPageSize, int maximalPageSize) {
      super(PointerSize.LONG, source, initialPageSize, maximalPageSize, PrimitivePortabilityImpl.LONG, port);
    }
  }

  static class FixedValLongEngine<V> implements StorageEngine<Long, V> {

    private final PrimitivePortability<V> vPort;
    private final BlockStorageArea fixedStore;
    private final int vSize;

    public FixedValLongEngine(PageSource ps, PrimitivePortability<V> vPort, int initialPageSize, int maxPageSize) {
      this.vPort = vPort;
      this.vSize = vPort.spaceNeededToEncode(null);
      this.fixedStore = new BlockStorageArea(ps, vSize + 8, initialPageSize, maxPageSize);
    }

    @Override
    public Long writeMapping(Long k, V v, int i, int i1) {
      ByteBuffer tb = ByteBuffer.allocate(vSize + 8);
      tb.putLong(k.longValue());
      vPort.encode(v, tb);
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
    public V readValue(long l) {
      ByteBuffer tb = fixedStore.blockAt(l);
      tb.position(tb.position() + Long.BYTES);
      return vPort.decode(tb);
    }

    @Override
    public boolean equalsValue(Object o, long l) {
      V val = readValue(l);
      return val != null && val.equals(o);
    }

    @Override
    public Long readKey(long l, int i) {
      ByteBuffer tb = fixedStore.blockAt(l);
      return tb.getLong(0);
    }

    @Override
    public boolean equalsKey(Object o, long l) {
      Long val = readKey(l, 0);
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
