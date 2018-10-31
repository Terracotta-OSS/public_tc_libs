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
package com.terracottatech.ehcache.clustered.server.offheap.frs.caches;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.PointerSize;

import com.terracottatech.ehcache.clustered.server.offheap.frs.OffHeapByteBufferCache;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A simple offheap caching strategy for <i>ByteBuffer</i> representation of chain
 * <p>
 *   Re-uses the same caching strategy used in previous versions for Hybrid
 * </p>
 */
public class OffHeapSimpleCachingStrategy implements OffHeapByteBufferCache {
  private static final long NULL_ENCODING = -1L;

  private static final int CACHE_PREVIOUS_OFFSET = 0;
  private static final int CACHE_NEXT_OFFSET = 8;
  private static final int CACHE_SOURCE_ADDRESS_OFFSET = 16;
  private static final int CACHE_ACCESS_COUNT_OFFSET = 24;
  private static final int CACHE_DATA_LENGTH_OFFSET = 28;
  private static final int CACHE_HEADER_SIZE = 32;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final OffHeapStorageArea cacheStorage;
  private volatile EventListener eventListener;

  private long first = NULL_ENCODING;
  private long last = NULL_ENCODING;
  private long hand = NULL_ENCODING;

  public OffHeapSimpleCachingStrategy(PageSource source, int pageSize) {
    this.cacheStorage = new OffHeapStorageArea(PointerSize.LONG, new CacheStorageOwner(), source, pageSize, false, true);
  }

  @Override
  public Long put(long sourceAddress, ByteBuffer value, int accessCount) {
    final int valueLength = value.remaining();
    lock.writeLock().lock();
    try {
      do {
        long cacheAddress = cacheStorage.allocate(valueLength + CACHE_HEADER_SIZE);
        if (cacheAddress >= 0) {
          cacheStorage.writeLong(cacheAddress + CACHE_SOURCE_ADDRESS_OFFSET, sourceAddress);
          cacheStorage.writeInt(cacheAddress + CACHE_DATA_LENGTH_OFFSET, valueLength);
          cacheStorage.writeBuffer(cacheAddress + CACHE_HEADER_SIZE, value.duplicate());
          linkEntry(cacheAddress);
          return cacheAddress;
        }
      } while (evict());
    } finally {
      lock.writeLock().unlock();
    }
    return null;
  }

  @Override
  public void setEventListener(EventListener eventListener) {
    this.eventListener = eventListener;
  }

  @SuppressWarnings({ "cast", "RedundantCast" })
  @Override
  public ByteBuffer get(long cacheAddress, long sourceAddress) {
    lock.readLock().lock();
    try {
      final long recordedSourceAddress = cacheStorage.readLong(cacheAddress + CACHE_SOURCE_ADDRESS_OFFSET);
      if (sourceAddress != recordedSourceAddress) {
        // this could happen on a free, which is on a write lock
        return null;
      }
      final int valueLength = cacheStorage.readInt(cacheAddress + CACHE_DATA_LENGTH_OFFSET);
      final ByteBuffer valueBuffer = cacheStorage.readBuffer(cacheAddress + CACHE_HEADER_SIZE, valueLength);
      return (ByteBuffer)ByteBuffer.allocate(valueLength).put(valueBuffer).flip();    // made redundant in Java 9/10
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public boolean remove(long cacheAddress, long sourceAddress) {
    return free(cacheAddress, sourceAddress);
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      cacheStorage.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public long getAllocatedMemory() {
    return cacheStorage.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return cacheStorage.getOccupiedMemory();
  }

  private boolean free(long cacheAddress, long givenSourceAddress) {
    lock.writeLock().lock();
    try {
      final long storedSourceAddress = cacheStorage.readLong(cacheAddress + CACHE_SOURCE_ADDRESS_OFFSET);
      if (givenSourceAddress != NULL_ENCODING) {
        // free only if given source address matches with stored source address
        if (storedSourceAddress != givenSourceAddress) {
          return false;
        }
      } else if (eventListener != null && storedSourceAddress != NULL_ENCODING) {
        eventListener.onEviction(storedSourceAddress);
      }
      unlinkEntry(cacheAddress);
      cacheStorage.free(cacheAddress);
      return true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void linkEntry(long cacheAddress) {
    if (hand == NULL_ENCODING) {
      cacheStorage.writeLong(cacheAddress + CACHE_PREVIOUS_OFFSET, last);
      cacheStorage.writeLong(cacheAddress + CACHE_NEXT_OFFSET, NULL_ENCODING);
      if (last == NULL_ENCODING) {
        first = cacheAddress;
      } else {
        cacheStorage.writeLong(last + CACHE_NEXT_OFFSET, cacheAddress);
      }
      last = cacheAddress;
    } else {
      long prev = cacheStorage.readLong(hand + CACHE_PREVIOUS_OFFSET);
      cacheStorage.writeLong(cacheAddress + CACHE_PREVIOUS_OFFSET, prev);
      if (prev == NULL_ENCODING) {
        first = cacheAddress;
      } else {
        cacheStorage.writeLong(prev + CACHE_NEXT_OFFSET, cacheAddress);
      }
      cacheStorage.writeLong(hand + CACHE_PREVIOUS_OFFSET, cacheAddress);
      cacheStorage.writeLong(cacheAddress + CACHE_NEXT_OFFSET, hand);
    }
  }

  private void unlinkEntry(long cacheAddress) {
    long prev = cacheStorage.readLong(cacheAddress + CACHE_PREVIOUS_OFFSET);
    long next = cacheStorage.readLong(cacheAddress + CACHE_NEXT_OFFSET);
    cacheStorage.writeLong(cacheAddress + CACHE_PREVIOUS_OFFSET, NULL_ENCODING);
    cacheStorage.writeLong(cacheAddress + CACHE_NEXT_OFFSET, NULL_ENCODING);
    cacheStorage.writeLong(cacheAddress + CACHE_SOURCE_ADDRESS_OFFSET, NULL_ENCODING);
    cacheStorage.writeLong(cacheAddress + CACHE_ACCESS_COUNT_OFFSET, 0);

    if (prev == NULL_ENCODING) {
      first = next;
    } else {
      cacheStorage.writeLong(prev + CACHE_NEXT_OFFSET, next);
    }
    if (next == NULL_ENCODING) {
      last = prev;
    } else {
      cacheStorage.writeLong(next + CACHE_PREVIOUS_OFFSET, prev);
    }

    if (hand == cacheAddress) {
      hand = next;
    }
  }

  private boolean evict() {
    while (true) {
      if (hand == NULL_ENCODING) {
        if (first == NULL_ENCODING) {
          return false;
        } else {
          hand = first;
        }
      }

      if (cacheStorage.readInt(hand + CACHE_ACCESS_COUNT_OFFSET) == 0) {
        return free(hand, NULL_ENCODING);
      } else {
        cacheStorage.writeInt(hand + CACHE_ACCESS_COUNT_OFFSET, 0);
        hand = cacheStorage.readLong(hand + CACHE_NEXT_OFFSET);
      }
    }
  }

  private class CacheStorageOwner implements OffHeapStorageArea.Owner {
    @Override
    public boolean evictAtAddress(long address, boolean shrink) {
      return free(address, NULL_ENCODING);
    }

    @Override
    public Lock writeLock() {
      return lock.writeLock();
    }

    @Override
    public boolean isThief() {
      return false;
    }

    @Override
    public boolean moved(long from, long to) {
      lock.writeLock().lock();
      try {
        long sourceAddress = cacheStorage.readLong(to + CACHE_SOURCE_ADDRESS_OFFSET);
        if (eventListener != null) {
          eventListener.onMove(sourceAddress, from, to);
        }

        long prev = cacheStorage.readLong(to + CACHE_PREVIOUS_OFFSET);
        long next = cacheStorage.readLong(to + CACHE_NEXT_OFFSET);
        if (prev == NULL_ENCODING) {
          first = to;
        } else {
          cacheStorage.writeLong(prev + CACHE_NEXT_OFFSET, to);
        }
        if (next == NULL_ENCODING) {
          last = to;
        } else {
          cacheStorage.writeLong(next + CACHE_PREVIOUS_OFFSET, to);
        }

        if (hand == from) {
          hand = to;
        }
        return true;
      } finally {
        lock.writeLock().unlock();
      }
    }

    //called under write lock
    @Override
    public int sizeOf(long cacheAddress) {
      lock.readLock().lock();
      try {
        int valueLength = cacheStorage.readInt(cacheAddress + CACHE_DATA_LENGTH_OFFSET);
        return CACHE_HEADER_SIZE + valueLength;
      } finally {
        lock.readLock().unlock();
      }
    }
  }
}