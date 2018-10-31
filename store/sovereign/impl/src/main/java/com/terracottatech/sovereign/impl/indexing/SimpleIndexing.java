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
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.spi.store.IndexMap;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.definition.CellDefinition;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.util.concurrent.Executors.callable;

/**
 * TODO error handling on creation of indexes.
 */
public class SimpleIndexing<K extends Comparable<K>> implements SovereignIndexing {

  private final SovereignDatasetImpl<K> dataset;
  private final ConcurrentSkipListMap<SimpleIndex<?, K>, SimpleIndex<?, K>> indexMap;

  public SimpleIndexing(SovereignDatasetImpl<K> dataset) {
    this.dataset = dataset;
    this.indexMap = new ConcurrentSkipListMap<>();
  }

  public SovereignDatasetImpl<K> getDataset() {
    return dataset;
  }

  @Override
  public synchronized <T extends Comparable<T>> Callable<SovereignIndex<T>> createIndex(CellDefinition<T> cellDefinition,
          SovereignIndexSettings settings) throws IllegalArgumentException {

    SimpleIndex<T, K> index = new SimpleIndex<>(cellDefinition, settings);
    if (!settings.isSorted() || settings.isUnique()) {
      throw new UnsupportedOperationException();
    }
    // exclusive lock it to manipulate creation, setup a deferred queue.
    index.exclusiveLock();
    try {
      SimpleIndex<?, K> prior = indexMap.putIfAbsent(index, index);
      if (prior != null) {
        throw new IllegalArgumentException("Index: " + index + " already exists");
      }
      try {
        index.create(dataset);
        index.markLoading(dataset);
      } catch (RuntimeException e) {
        try {
          destroyIndex(index);
        } catch (RuntimeException e1) {
          try {
            e.addSuppressed(e1);
          } catch (Exception e2) {
            // Ignore failure to add suppressed exception
          }
        }
        throw e;
      }
    } finally {
      index.exclusiveUnlock();
    }

    return callable(() -> {
      try (ContextImpl c = dataset.getContainer().start(false)) {
        index.sharedLock();
        try {
          index.populate(c, dataset);
        } finally {
          index.sharedUnlock();
        }
        index.exclusiveLock();
        try {
          index.postPopulate(c, dataset);
        } finally {
          index.exclusiveUnlock();
        }
      } catch (RuntimeException e) {
        // Index population failed -- need to unwind completely
        try{
          destroyIndex(index);
        } catch (RuntimeException e1) {
          try {
            e.addSuppressed(e1);
          } catch (Exception e2) {
            // Ignore failure to add suppressed exception
          }
        }
        throw e;
      }
    }, index);
  }

  @Override
  public synchronized <T extends Comparable<T>> void destroyIndex(SovereignIndex<T> index)
      throws StoreIndexNotFoundException {
    destroyIndex(index, true);
  }

  public synchronized <T extends Comparable<T>> void destroyIndex(SovereignIndex<T> index, boolean updateIndex)
      throws StoreIndexNotFoundException {
    SimpleIndex<?, K> old = indexMap.remove(index);
    if (old == null) {
      throw new StoreIndexNotFoundException("Cannot destroy " + index.definition() + ":" + index.on()
          + " index : does not exist");
    }
    old.exclusiveLock();
    try {
      old.destroy(this.dataset);
      if (updateIndex) {
        dataset.getConfig().getStorage().setMetadata(MetadataKey.Tag.DATASET_DESCR.keyFor(dataset.getUUID()),
                                                     this.dataset.getDescription());
      }
    } finally {
      old.exclusiveUnlock();
    }
  }

  @Override
  public List<SovereignIndex<?>> getIndexes() {
    return new ArrayList<>(indexMap.values());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Comparable<T>> SovereignIndex<T> getIndex(CellDefinition<T> cellDefinition,
                                                              SovereignIndexSettings settings) {
    SimpleIndex<T, K> probe = new SimpleIndex<>(cellDefinition, settings);
    return (SovereignIndex<T>) indexMap.get(probe);   // unchecked
  }

  public void recordIndex(ContextImpl c, Record<K> oldRecord, PersistentMemoryLocator oldLocator,
                          Record<K> newRecord, PersistentMemoryLocator newLocator) {
    assert !(oldRecord == null && newRecord == null);
    assert (oldRecord == null ? oldLocator == null : oldLocator != null);
    assert (newRecord == null ? newLocator == null : newLocator != null);
    for (SimpleIndex<?, K> simpleIndex : indexMap.values()) {
      simpleIndex.sharedLock();
      if (simpleIndex.isMutable()) {
        try {
          boolean isOld = oldRecord != null && oldRecord.get(simpleIndex.on()).isPresent();
          boolean isNew = newRecord != null && newRecord.get(simpleIndex.on()).isPresent();
          if (!isOld && isNew) {
            // add
            Object val = newRecord.get(simpleIndex.on()).get();
            simpleIndex.add(newRecord.getKey(), c, val, newLocator);
          } else if (isOld && !isNew) {
            // deletions
            Object val = oldRecord.get(simpleIndex.on()).get();
            simpleIndex.remove(oldRecord.getKey(), c, val, oldLocator);
          } else if (isOld && isNew) {
            // delta.
            Object oval = oldRecord.get(simpleIndex.on()).get();
            Object nval = newRecord.get(simpleIndex.on()).get();
            // if either the value changed or the location changed, update it.
            if (!oldLocator.equals(newLocator) || !oval.equals(nval)) {
              simpleIndex.update(newRecord.getKey(), c, oval, nval, newLocator);
            }
          }
          // noop. do nothing. here for clarity
        } catch (InterruptedException e) {
          // hmmm.
          // TODO what to do
        } finally {
          simpleIndex.sharedUnlock();
        }
      }
    }
  }

  public SimpleIndex<?, K> indexFor(IndexMap<?, ?, ?, ?> map) {
    List<SovereignIndex<?>> list = getIndexes();
    for (int i = 0; i < list.size(); i++) {
      try {
        @SuppressWarnings("unchecked")
        SimpleIndex<?, K> si = (SimpleIndex<?, K>) list.get(i);
        if (si.getUnderlyingIndex() == map) {
          return si;
        }
      } catch (Exception e) {
        // who cares?
      }
    }
    return null;
  }

  @Override
  public String toString() {
    StringWriter sw=new StringWriter();
    PrintWriter pw=new PrintWriter(sw);
    pw.println("SimpleIndexing for dataset=" + dataset);
    int cnt=0;
    for(SovereignIndex<?> si:getIndexes()) {
      if(cnt++>0) {
        pw.println();
      }
      pw.print(si);
    }
    pw.flush();
    return sw.toString();
  }

  public void buildIndexFor(int shardIdx) throws IOException {
    for(SimpleIndex<?, K> idx : indexMap.values()) {
      idx.exclusiveLock();
      try {
        idx.indexForShard(shardIdx, dataset);
      } finally {
        idx.exclusiveUnlock();
      }
    }
  }

}
