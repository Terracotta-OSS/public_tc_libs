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

package com.terracottatech.store.definition;

import com.terracottatech.store.Type;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;

/**
 * Definition interning implementation.
 */
class DefinitionInterner {

  private final AtomicReference<Thread> cleaner = new AtomicReference<>();
  private final StampedLock cleanerLock = new StampedLock();
  private final ConcurrentHashMap<String, CellDefinitionCache> cache = new ConcurrentHashMap<>();
  private final ReferenceQueue<CellDefinition<?>> clearedDefinitionReferences = new ReferenceQueue<>();
  private final BiFunction<String, Type<?>, CellDefinition<?>> generator;

  /**
   * Creates an interner using the supplied function as a source of definitions.
   *
   * @param generator source of definition instances
   */
  DefinitionInterner(BiFunction<String, Type<?>, CellDefinition<?>> generator) {
    this.generator = generator;
  }

  /**
   * Returns a interned definition with the supplied name and type.
   *
   * @param <T> cell definition JDK type
   * @param name cell definition name
   * @param type cell definition type
   * @return an interned cell definition
   */
  public <T> CellDefinition<T> intern(String name, Type<T> type) {
    long stamp = cleanerLock.tryOptimisticRead();
    /*
     * The CellDefinition use case tends to have a small population resulting
     * in some significant performance penalties in ConcurrentHashMap.computeIfAbsent.
     * Attempt a get first in an attempt to avoid computeIfAbsent.
     *
     * http://cs.oswego.edu/pipermail/concurrency-interest/2014-December/013360.html
     */
    final CellDefinitionCache definitionCache = cache.get(name);
    if (definitionCache != null) {
      return definitionCache.intern(type);
    }
    if (cache.isEmpty()) {
      startCleaner();
    }
    try {
      return cache.computeIfAbsent(name, key -> new SingleCellDefinitionCache<>(generator.apply(key, type))).intern(type);
    } finally {
      if (!cleanerLock.validate(stamp)) {
        startCleaner();
      }
    }
  }

  private void startCleaner() {
    Thread newCleaner = new Thread(new Cleaner(), "DefinitionInterner Cleaner");
    if (cleaner.compareAndSet(null, newCleaner)) {
      newCleaner.setDaemon(true);
      newCleaner.start();
    }
  }

  private interface CellDefinitionCache {

    public <U> CellDefinition<U> intern(Type<U> requestType);

    public CellDefinitionCache add(CellDefinition<?> definition);

    public CellDefinitionCache remove(WeakReference<?> definition);
  }

  private class SingleCellDefinitionCache<T> extends TaggedWeakReference<String, CellDefinition<T>> implements CellDefinitionCache {

    public SingleCellDefinitionCache(CellDefinition<T> defn) {
      super(defn.name(), defn, clearedDefinitionReferences);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> CellDefinition<U> intern(Type<U> requestType) {
      CellDefinition<T> cached = get();
      if (cached != null && requestType.equals(cached.type())) {
        return (CellDefinition<U>) cached;
      } else {
        String name = tag();
        CellDefinition<U> newDefn = (CellDefinition<U>) generator.apply(name, requestType);
        if (cache.replace(name, this, add(newDefn))) {
          return newDefn;
        } else {
          return DefinitionInterner.this.intern(name, requestType);
        }
      }
    }

    @Override
    public CellDefinitionCache add(CellDefinition<?> definition) {
      CellDefinition<?> current = get();
      if (current == null) {
        return new SingleCellDefinitionCache<>(definition);
      } else {
        return new MultipleCellDefinitionCache(tag(), current, definition);
      }
    }

    @Override
    public CellDefinitionCache remove(WeakReference<?> reference) {
      if (reference == this) {
        return null;
      } else {
        return this;
      }
    }
  }

  private class MultipleCellDefinitionCache implements CellDefinitionCache {

    private final String name;
    private final Map<Type<?>, TaggedWeakReference<String, CellDefinition<?>>> definitions;

    public MultipleCellDefinitionCache(String name, CellDefinition<?> first, CellDefinition<?> second) {
      this.name = name;
      Map<Type<?>, TaggedWeakReference<String, CellDefinition<?>>> map = new HashMap<>(2);
      map.put(first.type(), new TaggedWeakReference<>(name, first, clearedDefinitionReferences));
      map.put(second.type(), new TaggedWeakReference<>(name, second, clearedDefinitionReferences));
      this.definitions = Collections.unmodifiableMap(map);
    }

    private MultipleCellDefinitionCache(String name, Map<Type<?>, TaggedWeakReference<String, CellDefinition<?>>> map) {
      this.name = name;
      this.definitions = Collections.unmodifiableMap(map);
    }

    @Override
    public <U> CellDefinition<U> intern(Type<U> requestType) {
      TaggedWeakReference<String, CellDefinition<?>> wr = definitions.get(requestType);

      if (wr != null) {
        @SuppressWarnings("unchecked")
        CellDefinition<U> defn = (CellDefinition<U>) wr.get();
        if (defn != null) {
          return defn;
        }
      }

      @SuppressWarnings("unchecked")
      CellDefinition<U> defn = (CellDefinition<U>) generator.apply(name, requestType);
      if (cache.replace(name, this, add(defn))) {
        return defn;
      } else {
        return DefinitionInterner.this.intern(name, requestType);
      }
    }

    @Override
    public CellDefinitionCache add(CellDefinition<?> definition) {
      Map<Type<?>, TaggedWeakReference<String, CellDefinition<?>>> map = new HashMap<>(definitions.size() + 1);
      for (Entry<Type<?>, TaggedWeakReference<String, CellDefinition<?>>> e : definitions.entrySet()) {
        if (e.getValue().get() != null) {
          map.put(e.getKey(), e.getValue());
        }
      }
      if (map.isEmpty()) {
        return new SingleCellDefinitionCache<>(definition);
      } else {
        map.put(definition.type(), new TaggedWeakReference<>(name, definition, clearedDefinitionReferences));
        return new MultipleCellDefinitionCache(name, map);
      }
    }

    @Override
    public CellDefinitionCache remove(WeakReference<?> reference) {
      Map<Type<?>, TaggedWeakReference<String, CellDefinition<?>>> map = new HashMap<>(definitions.size() - 1);
      for (Entry<Type<?>, TaggedWeakReference<String, CellDefinition<?>>> e : definitions.entrySet()) {
        if (e.getValue() != reference && e.getValue().get() != null) {
          map.put(e.getKey(), e.getValue());
        }
      }
      if (map.size() == definitions.size()) {
        return this;
      } else {
        switch (map.size()) {
          case 0: return null;
          case 1: {
            CellDefinition<?> definition = map.values().iterator().next().get();
            if (definition == null) {
              return null;
            } else {
              return new SingleCellDefinitionCache<>(definition);
            }
          }
          default: return new MultipleCellDefinitionCache(name, map);
        }
      }
    }
  }

  private static class TaggedWeakReference<T, R> extends WeakReference<R> {

    private final T tag;

    public TaggedWeakReference(T tag, R referent, ReferenceQueue<? super R> q) {
      super(referent, q);
      this.tag = tag;
    }

    public T tag() {
      return tag;
    }
  }

  private class Cleaner implements Runnable {

    @Override
    public void run() {
      try {
        while (true) {
          Reference<?> cleared = clearedDefinitionReferences.remove();
          if (cleared instanceof TaggedWeakReference<?, ?>) {
            @SuppressWarnings("unchecked")
            TaggedWeakReference<String, CellDefinition<?>> twr = (TaggedWeakReference<String, CellDefinition<?>>) cleared;
            String name = twr.tag();
            CellDefinitionCache cdc = cache.get(name);
            if (cdc != null) {
              CellDefinitionCache newCdc = cdc.remove(twr);
              if (newCdc == null) {
                cache.remove(name, cdc);
              } else if (newCdc != cdc) {
                cache.replace(name, cdc, newCdc);
              }
            }
          } else {
            throw new AssertionError("Unexpected reference type in queue");
          }

          if (cache.isEmpty()) {
            long stamp = cleanerLock.writeLock();
            try {
              if (cache.isEmpty()) {
                return;
              }
            } finally {
              cleanerLock.unlockWrite(stamp);
            }
          }
        }
      } catch (InterruptedException ex) {
        //ignore
      } finally {
        long stamp = cleanerLock.writeLock();
        try {
          cache.clear();
          cleaner.set(null);
        } finally {
          cleanerLock.unlockWrite(stamp);
        }
      }
    }
  }
}
