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

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class SlidingWindow<T> {
  private final T[] window;
  private final Map<T, Integer> indexMap;
  private final Consumer<T> consumeWhenSlid;
  private int currentIndex;

  public SlidingWindow(Class<T> clazz, int size, Consumer<T> consumeWhenSlid) {
    this.consumeWhenSlid = consumeWhenSlid;
    this.indexMap = new HashMap<>();
    @SuppressWarnings("unchecked")
    T[] a = (T[])Array.newInstance(clazz, size);
    this.window = a;
    for (int i = 0; i < this.window.length; i++) {
      this.window[i] = null;
    }
    this.currentIndex = 0;
  }

  public void putItem(T item) {
    int idx = currentIndex % window.length;
    currentIndex = (currentIndex + 1) & Integer.MAX_VALUE;
    T old = window[idx];
    window[idx] = item;
    if (old != null) {
      indexMap.remove(item);
      consumeWhenSlid.accept(old);
    }
    indexMap.put(item, idx);
  }

  public void clearItem(T item) {
    Integer idx = indexMap.remove(item);
    if (idx != null) {
      T old = window[idx];
      window[idx] = null;
      if (old != null) {
        consumeWhenSlid.accept(old);
      }
    }
  }

  public void clearAll() {
    for (int i = 0; i < window.length; i++) {
      T item = window[i];
      if (item != null) {
        consumeWhenSlid.accept(item);
      }
      window[i] = null;
    }
    indexMap.clear();
  }
}
