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
package com.terracottatech.ehcache.clustered.server.offheap.frs;

import java.nio.ByteBuffer;

/**
 * Caching interface for caching byte buffer representation of chain.
 * <p>
 *   Always assumes longs as keys due to offheap addresses.
 *   This interface allows different caching strategies to be plugged in to the hybrid storage engine in the future.
 *   e.g a hot spot detecting cache which keeps the hottest items in the cache based on a min-heap tree.
 * </p>
 */
public interface OffHeapByteBufferCache {
  /**
   * Set an event listener so that cache events can be notified back to the hybrid storage engine
   *
   * @param eventListener OffHeap hybrid cache event listener
   */
  void setEventListener(EventListener eventListener);

  /**
   * Puts an item into the cache.
   * <p>
   *   Depending on the underlying caching strategy, the strategy may decide not to put the item in the cache
   * </p>
   * @param sourceAddress Address of the offheap source within the hybrid engine
   * @param value the byte buffer equivalent of chain
   * @param accessCount number of times this chain was accessed. Useful for writing hot-spot based strategies
   * @return An address if cached, null otherwise
   */
  Long put(long sourceAddress, ByteBuffer value, int accessCount);

  /**
   * Gets the cached value in raw byte buffer form.
   *
   * @param cacheAddress Address of the entry
   * @param sourceAddress Address of the mapped storage engine entry
   *
   * @return data value in byte buffer form, null otherwise
   */
  ByteBuffer get(long cacheAddress, long sourceAddress);

  /**
   * Removes the entry from cache.
   *
   * @param cacheAddress address of the entry in cache
   * @param sourceAddress address of the entry in storage engine
   *
   * @return true if removed, false otherwise
   */
  boolean remove(long cacheAddress, long sourceAddress);

  void clear();

  long getAllocatedMemory();

  long getOccupiedMemory();
  /**
   * {@code EventListener} interface that must be implemented by storage engine(s) that uses
   * any of the caching strategies.
   */
  interface EventListener {
    void onEviction(long sourceAddress);
    void onMove(long sourceAddress, long fromCacheAddress, long toCacheAddress);
  }
}