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
package com.terracottatech.sovereign.impl.indexing;

import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.RecordBufferStrategy;
import com.terracottatech.sovereign.impl.memory.ShardIterator;
import com.terracottatech.sovereign.impl.memory.ShardedBtreeIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignSecondaryIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexStatistics;
import com.terracottatech.sovereign.spi.IndexMapConfig;
import com.terracottatech.sovereign.spi.store.DataContainer;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * @author cschanck
 */
public class SimpleIndex<T extends Comparable<T>, K extends Comparable<K>> implements SovereignIndex<T> {

  public static final boolean BULK_INDEX_CREATION = true;

  enum Operation { ADD, REMOVE, REPLACE }
  class Op {
    final Operation operation;
    final T oldValue;
    final T newValue;
    final PersistentMemoryLocator loc;
    final K key;

    public Op(Operation operation, T oldValue, T newValue, PersistentMemoryLocator loc, K key) {
      this.operation=operation;
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.loc = loc;
      this.key = key;
    }

  }

  private ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();
  private final CellDefinition<T> def;
  private final IndexSettingsWrapper settings;
  private SovereignSecondaryIndexMap<T, K> underlyingIndex;
  private volatile State state = State.UNKNOWN;
  private ConcurrentLinkedQueue<Op> deferredQueue = new ConcurrentLinkedQueue<>();
  private SovereignIndexStatistics stats;

  public SimpleIndex(CellDefinition<T> def, SovereignIndexSettings settings) {
    this.def = def;
    this.settings = new IndexSettingsWrapper(settings);
    this.stats = settings.isSorted() ? new SimpleIndexSortedStatistics(this) : new SimpleIndexStatistics(this);
  }

  private void clearStats() {
    this.stats = settings.isSorted() ? SimpleIndexSortedStatistics.NOOP : SimpleIndexStatistics.NOOP;
  }

  public SimpleIndex(SimpleIndexDescription<T> descr) {
    this(CellDefinition.define(descr.getCellName(), descr.getCellType()), descr.getIndexSettings());
  }

  @Override
  public CellDefinition<T> on() {
    return def;
  }

  @Override
  public SovereignIndexSettings definition() {
    return settings.getSettings();
  }

  @Override
  public boolean isLive() {
    return state == State.LIVE;
  }

  public void destroy(final SovereignDatasetImpl<K> dataset) {
    switch (state) {
      case UNKNOWN:
      case CREATED:
      case LOADING:
      case LIVE:
        state = State.UNKNOWN;
        SovereignIndexMap<T, K> tmp = underlyingIndex;
        underlyingIndex = null;
        if (tmp != null) {
          dataset.getUsage().removeMap(tmp);
          tmp.drop();
        }
        clearStats();
        break;
      default:
        throw new IllegalStateException();
    }
  }

  public void create(SovereignDatasetImpl<K> dataset) {
    IndexMapConfig<T> config = new IndexMapConfig<T>() {
      @Override
      public boolean isSortedMap() {
        return definition().isSorted();
      }

      @Override
      public Type<T> getType() {
        return on().type();
      }

      @Override
      public DataContainer<?, ?, ?> getContainer() {
        return dataset.getContainer();
      }

    };
    switch (state) {
      case UNKNOWN:
        underlyingIndex = getMap(dataset, config);
        state = State.CREATED;
        dataset.getConfig().getStorage().setMetadata(MetadataKey.Tag.DATASET_DESCR.keyFor(dataset.getUUID()),
                                                     dataset.getDescription());
        break;
      case CREATED:
      case LOADING:
      case LIVE:
      default:
        throw new IllegalStateException("Index state expected to be Unknown");
    }
  }

  @SuppressWarnings("unchecked")
  private SovereignSecondaryIndexMap<T, K> getMap(SovereignDatasetImpl<K> dataset, IndexMapConfig<T> config) {
    return (SovereignSecondaryIndexMap<T, K>) dataset.getUsage().createMap(def.toString(), config);
  }

  public void populate(ContextImpl c, SovereignDatasetImpl<K> dataset) {
    switch (state) {
      case UNKNOWN:
      case CREATED:
      case LIVE:
        throw new IllegalStateException("Can't populate a " + state.name() + " index.");
      case LOADING:
        try {
          rawPopulate(c, dataset);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new IllegalStateException();
    }
  }

  public void postPopulate(ContextImpl c, SovereignDatasetImpl<K> dataset) {
    switch (state) {
      case UNKNOWN:
      case CREATED:
      case LIVE:
        throw new IllegalStateException("Can't populate a " + state.name() + " index.");
      case LOADING:
        Op op;
        while ((op = deferredQueue.poll()) != null) {
          switch(op.operation) {
            case ADD:
              underlyingIndex.put(op.key, c, op.newValue, op.loc);
              break;
            case REMOVE:
              underlyingIndex.remove(op.key, c, op.oldValue, op.loc);
              break;
            case REPLACE:
              if(!underlyingIndex.replace(op.key, c, op.oldValue, op.newValue, op.loc)) {
                throw new AssertionError();
              }
              break;
          }
        }
        state = State.LIVE;
        dataset.getConfig().getStorage().setMetadata(MetadataKey.Tag.DATASET_DESCR.keyFor(dataset.getUUID()),
                                                     dataset.getDescription());
        break;
      default:
        throw new IllegalStateException();
    }

  }

  public void indexForShard(int shardIdx, SovereignDatasetImpl<K> dataset) throws IOException {
    if (state != State.LIVE) {
      throw new IllegalStateException("Can't build shard wise indexes due to invalid state" + state.name());
    }
    SovereignIndexMap<T, K> underlying = getUnderlyingIndex();
    if (underlying instanceof SovereignSortedIndexMap && BULK_INDEX_CREATION) {
      SovereignSortedIndexMap<T, K> sorted= (SovereignSortedIndexMap<T, K>) underlying;
      SovereignSortedIndexMap.BatchHandle<T> batch = ((ShardedBtreeIndexMap<T, K>)sorted).getShards().get(shardIdx).batch();

      ShardIterator iterator = dataset.iterate(shardIdx);
      RecordBufferStrategy<K> bufferStrategy = dataset.getRuntime().getBufferStrategy();

      while (iterator.hasNext()) {
        BufferDataTuple next = iterator.next();
        Cell<?> cell = bufferStrategy.fromByteBuffer(next.getData()).cells().get(on().name());
        if (cell != null) {
          @SuppressWarnings("unchecked")
          T val = (T) cell.value();
          batch.batchAdd(bufferStrategy.readKey(next.getData()), val, new PersistentMemoryLocator(next.index(), null));
        }

      }
      batch.process();
      batch.close();
    }
  }

  private void rawPopulate(ContextImpl c, SovereignDatasetImpl<K> dataset) throws IOException {
    try (final Stream<Record<K>> recordStream = dataset.records()) {
      SovereignIndexMap<T, K> underlying = getUnderlyingIndex();

      if (underlying instanceof SovereignSortedIndexMap && BULK_INDEX_CREATION) {
        SovereignSortedIndexMap<T, K> sorted= (SovereignSortedIndexMap<T, K>) underlying;
        SovereignSortedIndexMap.BatchHandle<T> batch = sorted.batch();
        recordStream.filter(r -> r.get(on()).isPresent()).forEach(cells -> {
          PersistentMemoryLocator loc = dataset.getPrimary().get(c, cells.getKey());
          T val = cells.get(on()).get();
          batch.batchAdd(cells.getKey(), val, loc);
        });
        batch.process();
        batch.close();
      } else {
        recordStream.filter(r -> r.get(on()).isPresent()).forEach(cells -> {
          PersistentMemoryLocator loc = dataset.getPrimary().get(c, cells.getKey());
          T val = cells.get(on()).get();
          getUnderlyingIndex().put(cells.getKey(), c, val, loc);
        });
      }
    }
  }

  public SovereignIndexMap<T, K> getUnderlyingIndex() {
    return underlyingIndex;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleIndex<?, ?> index = (SimpleIndex<?, ?>) o;

    if (def != null ? !def.equals(index.def) : index.def != null) {
      return false;
    }

    if (settings != null ? !settings.equals(index.settings) : index.settings != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = def != null ? def.hashCode() : 0;
    result = 31 * result + (settings != null ? settings.hashCode() : 0);
    return result;
  }

  @Override
  public int compareTo(SovereignIndex<?> o) {
    // name
    int ret = on().name().compareTo(o.on().name());
    if (ret != 0) {
      return ret;
    }
    // type
    ret = on().type().getJDKType().getName().compareTo(o.on().type().getJDKType().getName());
    if (ret != 0) {
      return ret;
    }
    return IndexSettingsWrapper.staticCompareTo(settings.getSettings(), o.definition());
  }

  @Override
  public String toString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("Index on " + def + " is " + state+ "; settings=" + settings);
    pw.print(getStatistics());
    pw.flush();
    return sw.toString();
  }

  @SuppressWarnings("unchecked")
  public void add(K key, ContextImpl c, Object val, PersistentMemoryLocator loc) throws InterruptedException {
    switch (state) {
      case LIVE:
        underlyingIndex.put(key, c, (T) val, loc);
        break;
      case LOADING:
        deferredQueue.add(new Op(Operation.ADD, null, (T) val, loc, key));
        break;
      case UNKNOWN:
      case CREATED:
        throw new IllegalStateException("Can't add to a " + state.name() + " index.");
      default:
        throw new IllegalStateException("Unhandled state: " + state.name());
    }
  }

  @SuppressWarnings("unchecked")
  public void remove(K key, ContextImpl c, Object val, PersistentMemoryLocator loc) throws InterruptedException {
    switch (state) {
      case LIVE:
        underlyingIndex.remove(key, c, (T) val, loc);
        break;
      case LOADING:
        deferredQueue.add(new Op(Operation.REMOVE, (T) val, null, loc, key));
        break;
      case UNKNOWN:
      case CREATED:
        throw new IllegalStateException("Can't add to a " + state.name() + " index.");
      default:
        throw new IllegalStateException("Unhandled state: " + state.name());
    }
  }

  @SuppressWarnings("unchecked")
  public void update(K key, ContextImpl c,
                     Object oldVal,
                     Object newVal,
                     PersistentMemoryLocator loc) throws InterruptedException {
    switch (state) {
      case LIVE:
        if(!underlyingIndex.replace(key, c, (T) oldVal, (T) newVal, loc)) {
          throw new AssertionError();
        }
        break;
      case LOADING:
        deferredQueue.add(new Op(Operation.REPLACE, (T) oldVal, (T) newVal, loc, key));
        break;
      case UNKNOWN:
      case CREATED:
        throw new IllegalStateException("Can't add to a " + state.name() + " index.");
      default:
        throw new IllegalStateException("Unhandled state: " + state.name());
    }
  }

  public void sharedLock() {
    indexLock.readLock().lock();
  }

  public void sharedUnlock() {
    indexLock.readLock().unlock();
  }

  public void exclusiveLock() {
    indexLock.writeLock().lock();
  }

  public void exclusiveUnlock() {
    indexLock.writeLock().unlock();
  }

  public void markLoading( SovereignDatasetImpl<?> dataset) {
    switch (state) {
      case CREATED:
        deferredQueue.clear();
        state = State.LOADING;
        dataset.getConfig().getStorage().setMetadata(MetadataKey.Tag.DATASET_DESCR.keyFor(dataset.getUUID()),
                                                     dataset.getDescription());

        break;
      case LIVE:
      case LOADING:
      case UNKNOWN:
        throw new IllegalStateException("Can't mark loading for a " + state.name() + " index.");
      default:
        throw new IllegalStateException("Unhandled state: " + state.name());
    }
  }

  public boolean isMutable() {
    return state == State.LIVE || state == State.LOADING;
  }

  @Override
  public SimpleIndexDescription<T> getDescription() {
    return new SimpleIndexDescription<>(state, def.name(), def.type(), settings.getSettings());
  }

  @Override
  public SovereignIndexStatistics getStatistics() {
    return stats;
  }

}
