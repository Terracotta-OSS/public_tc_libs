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
package com.terracottatech.sovereign.common.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

/**
 * Simplest lRU cache, based on LinkedHashMap.
 *
 * @param <K> the type parameter
 * @param <V> the type parameter
 * @author cschanck
 */
@SuppressWarnings("serial")
public class SimplestLRUCache<K, V> extends LinkedHashMap<K, V> {
  private final int capacity;

  /**
   * Instantiates a new Simplest lRU cache.
   *
   * @param capacity the capacity
   */
  public SimplestLRUCache(int capacity) {
    super(capacity, 0.75f, true);
    this.capacity = capacity;
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() > capacity;
  }

  public static class ThreadSafe<K, V> {
    private final Map<K, V> cache;
    private Lock lock = new StampedLock().asWriteLock();

    public ThreadSafe(int capacity) {
      this.cache = new SimplestLRUCache<>(capacity);
    }

    public V get(K key) {
      lock.lock();
      try {
        return cache.get(key);
      } finally {
        lock.unlock();
      }
    }

    public V remove(K key) {
      lock.lock();
      try {
        return cache.remove(key);
      } finally {
        lock.unlock();
      }
    }

    public V put(K key, V val) {
      lock.lock();
      try {
        return cache.put(key, val);
      } finally {
        lock.unlock();
      }
    }

    public void clear() {
      lock.lock();
      try {
        cache.clear();
      } finally {
        lock.unlock();
      }
    }
  }

  public static class Segmented<K, V> {
    private final ThreadSafe<K, V>[] segments;

    @SuppressWarnings("unchecked")
    public Segmented(int segmentCount, int capacity) {
      segmentCount = Math.max(1, Integer.highestOneBit(segmentCount));
      this.segments = (ThreadSafe<K, V>[]) new ThreadSafe<?, ?>[segmentCount];
      for (int i = 0; i < segments.length; i++) {
        segments[i] = new ThreadSafe<>(Math.max(10, capacity / segmentCount));
      }
    }

    private ThreadSafe<K, V> cacheFor(K key) {
      return segments[key.hashCode() & (segments.length - 1)];
    }

    public V get(K key) {
      return cacheFor(key).get(key);
    }

    public V remove(K key) {
      return cacheFor(key).remove(key);
    }

    public V put(K key, V val) {
      return cacheFor(key).put(key, val);
    }

    public void clear() {
      for (ThreadSafe<K, V> ts : segments) {
        ts.clear();
      }
    }
  }

}
