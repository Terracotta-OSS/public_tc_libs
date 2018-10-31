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
package com.terracottatech.sovereign.impl.dataset.metadata;

import com.terracottatech.store.definition.CellDefinition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * @author cschanck
 **/
public class DatasetSchemaBackend extends AbstractSchema  {
  private static final int MAX_SCHEMA_DB_SIZE = Short.MAX_VALUE;
  private final ConcurrentMap<CellDefinition<?>, SchemaCellDefinition<?>> cDefToSchemaMap = (ConcurrentMap<CellDefinition<?>, SchemaCellDefinition<?>>) getDefToSchemaMap();
  private final ConcurrentMap<Integer, CellDefinition<?>> cIdToSchemaMap = (ConcurrentMap<Integer, CellDefinition<?>>) getIdToSchemaMap();
  private AtomicInteger idGen = new AtomicInteger(0);
  private Consumer<DatasetSchemaBackend> callback = (r) -> {};
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private ConcurrentHashMap<DatasetSchemaThreadLocal, DatasetSchemaThreadLocal> locals = new ConcurrentHashMap<>();

  public DatasetSchemaBackend() {
    super();
  }

  public void setCallback(Consumer<DatasetSchemaBackend> cb) {
    lock.writeLock().lock();
    try {
      this.callback = cb;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Map<CellDefinition<?>, SchemaCellDefinition<?>> makeDefToSchemaMap() {
    return new ConcurrentHashMap<>(16, 0.75f, Runtime.getRuntime().availableProcessors() * 2);
  }

  @Override
  public Map<Integer, CellDefinition<?>> makeIdToSchemaMap() {
    return new ConcurrentHashMap<>(16, 0.75f, Runtime.getRuntime().availableProcessors() * 2);
  }

  @Override
  public SchemaCellDefinition<?> idFor(CellDefinition<?> def) {
    // fine with this heavy synchro here, as it will become irrelevant really fast.
    // fast path
    if (isOverflowed()) {
      return null;
    }
    lock.writeLock().lock();
    try {
      SchemaCellDefinition<?> probe = cDefToSchemaMap.get(def);
      if (probe != null) {
        return probe;
      }
      // allocate one
      SchemaCellDefinition<?> cand = new SchemaCellDefinition<>(def, idGen.incrementAndGet());
      cIdToSchemaMap.put(cand.id(), cand.definition());
      cDefToSchemaMap.put(cand.definition(), cand);
      callback.accept(this);
      return cand;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Map<CellDefinition<?>, SchemaCellDefinition<?>> getContents() {
    return cDefToSchemaMap;
  }

  public int getCurrentLastMaxId() {
    return idGen.get();
  }

  @Override
  public boolean isOverflowed() {
    return cDefToSchemaMap.size() >= MAX_SCHEMA_DB_SIZE;
  }

  public void register(DatasetSchemaThreadLocal local) {
    locals.put(local, local);
  }

  public PersistableSchemaList getPersistable() {
    lock.readLock().lock();
    try {
      return new PersistableSchemaList(this);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void dispose() {
    lock.writeLock().lock();
    try {
      // kill the callback;
      callback = (r) -> {};
      // scrub the locals
      ConcurrentHashMap<DatasetSchemaThreadLocal, DatasetSchemaThreadLocal> tmp = locals;
      locals = new ConcurrentHashMap<>();
      for (DatasetSchemaThreadLocal t : tmp.values()) {
        t.dispose();
      }
      cDefToSchemaMap.clear();
      cIdToSchemaMap.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void reload(PersistableSchemaList seed) {
    lock.writeLock().lock();
    try {
      dispose();
      setTo(seed);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public CellDefinition<?> definitionFor(int id) {
    lock.readLock().lock();
    try {
      return super.definitionFor(id);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setTo(PersistableSchemaList seed) {
    lock.writeLock().lock();
    try {
      int hwMark = Integer.MIN_VALUE;
      for (SchemaCellDefinition<?> def : seed.getDefinitions()) {
        if (def.id() > hwMark) {
          hwMark = def.id();
        }
        cDefToSchemaMap.put(def.definition(), def);
        cIdToSchemaMap.put(def.id(), def.definition());
      }
      idGen.set(hwMark + 1);
    } finally {
      lock.writeLock().unlock();
    }
  }
}
