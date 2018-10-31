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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.sovereign.impl.SovereignAllocationResource.BufferAllocator;
import com.terracottatech.sovereign.impl.memory.KeySlotShardEngine.LongToLong;
import com.terracottatech.sovereign.spi.store.Context;
import com.terracottatech.sovereign.impl.utils.BitSetAddressList;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;


/**
 * @author cschanck (at this point, myron would recognize it)
 */
public class MemoryAddressList implements AddressList {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryAddressList.class);

  private static final int BITS_PER_CHUNK = 16;
  private final Consumer<Long> freeOp;
  private final int minimumGCCount;
  private final BitSetAddressList storage;
  private final LongToLong addOffsetMask;
  private final LongToLong removeOffsetmask;
  private volatile boolean isDisposed = false;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock rlock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock wlock = lock.writeLock();

  MemoryAddressList(BufferAllocator source, Consumer<Long> freeOp, int minimumGCCount) {
    this(source, freeOp, minimumGCCount, KeySlotShardEngine.NOOP_LTOL, KeySlotShardEngine.NOOP_LTOL);
  }

  MemoryAddressList(BufferAllocator source,
                    Consumer<Long> freeOp,
                    int minimumGCCount,
                    LongToLong addOffsetMask,
                    LongToLong removeOffsetMask) {
    storage = new BitSetAddressList(source, BITS_PER_CHUNK);
    this.minimumGCCount = minimumGCCount;
    this.freeOp = freeOp;
    this.addOffsetMask = addOffsetMask;
    this.removeOffsetmask = removeOffsetMask;
  }

  public long size() {
    return storage.size();
  }
  @Override
  public long get(long index) {
    rlock.lock();
    try {
      this.testDisposed();
      long transmutedIndex = -1;
      try {
        transmutedIndex = removeOffsetmask.transmute(index);
        long ret = storage.get(transmutedIndex);
        if (ret >= 0) {
          return ret;
        }
        return -1l;
      } catch (IndexOutOfBoundsException e) {
        LOGGER.debug("Out of bound index: transmuted: {}, original: {}", transmutedIndex, index);
        return -1l;
      }
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public long put(long add) {
    wlock.lock();
    try {
      testDisposed();
      if (add == 0) {
        throw new IllegalArgumentException("invalid uid");
      }
      long slot = storage.reserve();
      storage.set(slot, add);
      return addOffsetMask.transmute(slot);
    } finally {
      wlock.unlock();
    }
  }

  @Override
  public void tradeInPlace(long index, long data) {
    wlock.lock();
    try {
      this.testDisposed();
      index = removeOffsetmask.transmute(index);
      long prior = storage.get(index);
      if (prior < 0) {
        throw new IllegalStateException();
      }
      storage.reset(index, data);
      freeOp.accept(prior);
    } finally {
      wlock.unlock();
    }
  }

  public void finishInitialization() {
    wlock.lock();
    try {
      this.testDisposed();
      storage.initializeFreeListByScan();
    } finally {
      wlock.unlock();
    }
  }

  public long reserve() {
    wlock.lock();
    try {
      testDisposed();
      return addOffsetMask.transmute(storage.reserve());
    } finally {
      wlock.unlock();
    }
  }

  public void reinstall(long index, long value) {
    wlock.lock();
    try {
      this.testDisposed();
      if (value == 0) {
        throw new AssertionError("invalid uid");
      }
      index = removeOffsetmask.transmute(index);
      long slot = storage.reserve(index);
      storage.set(slot, value);
    } finally {
      wlock.unlock();
    }
  }

  public void restore(long index, long value) {
    wlock.lock();
    try {
      this.testDisposed();
      if (value == 0) {
        throw new AssertionError("invalid uid");
      }
      index = removeOffsetmask.transmute(index);

      long slot = -1;
      try {
        slot = storage.get(index);
      } catch (IndexOutOfBoundsException e) {
        // no op
      }
      if (slot >= 0) {
        storage.reset(index, value);
        freeOp.accept(slot);
      } else {
        slot = storage.reserve(index);
        storage.set(slot, value);
      }
    } finally {
      wlock.unlock();
    }
  }

  public boolean contains(long index) {
    rlock.lock();
    try {
      this.testDisposed();
      index = removeOffsetmask.transmute(index);
      return storage.get(index) >= 0;
    } catch (IndexOutOfBoundsException e) {
      return false;
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public long clear(long index) {
    wlock.lock();
    try {
      this.testDisposed();
      index = removeOffsetmask.transmute(index);
      long uid = storage.get(index);
      storage.clear(index);
      freeOp.accept(uid);
      return uid;
    } finally {
      wlock.unlock();
    }
  }

  public void clear() {
    wlock.lock();
    try {
      this.testDisposed();
      storage.clear();
    } finally {
      wlock.unlock();
    }
  }

  @Override
  public String toString() {
    rlock.lock();
    try {
      if (isDisposed) {
        return "Disposed";
      }
      StringBuilder builder = new StringBuilder();
      for (long x : storage) {
        builder.append(storage.get(x));
        builder.append(" ");
      }
      return builder.toString();
    } finally {
      rlock.unlock();
    }
  }

  public void place(long slot, long lsn) {
    rlock.lock();
    try {
      testDisposed();
      long index = removeOffsetmask.transmute(slot);
      storage.set(index, lsn);
    } finally {
      rlock.unlock();
    }
  }

  @SuppressWarnings("try")
  interface ContextIterator extends AutoCloseable, Iterator<Long> {
  }

  @Override
  public ContextIterator iterator(final Context context) {
    this.testDisposed();
    return new ContextIterator() {

      private BitSetAddressList.TrackingIterator base;
      private long next = -1L;
      private final Context ctx = context;    // Held for liveliness

      {
        try {
          wlock.lock();
          try {
            base = MemoryAddressList.this.storage.iterator();
          } finally {
            wlock.unlock();
          }
          advance();
        } finally {
          ctx.addCloseable(this);
        }
      }

      @Override
      public void close() {
      }

      private void advance() {
        if (base != null && next < 0) {
          rlock.lock();
          try {
            next = -1;
            while (base.hasNext()) {
              next = base.next();
              break;
            }
          } finally {
            rlock.unlock();
          }
        }
      }

      @Override
      public boolean hasNext() {
        return (next >= 0);
      }

      @Override
      public Long next() {
        MemoryAddressList.this.testDisposed();
        if (hasNext()) {
          long ret = next;
          next = -1L;
          advance();
          return addOffsetMask.transmute(ret);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  /**
   * Marks this {@code MemoryAddressList} unusable and destroys internal
   * data structures.
   */
  public void dispose() {
    wlock.lock();
    try {
      if (this.isDisposed) {
        return;
      }
      this.isDisposed = true;
      this.storage.clear();
    } finally {
      wlock.unlock();
    }
  }

  private void testDisposed() throws IllegalStateException {
    if (this.isDisposed) {
      throw new IllegalStateException("Attempt to use disposed address list");
    }
  }

  public long getAllocatedSlots() {
    return storage.getAllocatedSlots();
  }

}
