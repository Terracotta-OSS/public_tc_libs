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

import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.model.PersistableDataContainer;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.spi.store.ContainerPersistenceBroker;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.sovereign.spi.store.LocatorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @author cschanck
 **/
public class ShardedRecordContainer<K extends Comparable<K>, C extends AbstractRecordContainer<K>>
  implements SovereignContainer<K>, PersistableDataContainer<K, ContainerPersistenceBroker> {

  private final C[] shards;
  private final SovereignRuntime<K> runtime;
  private final KeySlotShardEngine shardEngine;
  private final KeySlotShardEngine.LongToInt shardIndexFromSlot;

  public ShardedRecordContainer(C[] shards) {
    this.shards = Arrays.copyOf(shards, shards.length);
    this.runtime = this.shards[0].runtime();
    this.shardEngine = runtime.getShardEngine();
    this.shardIndexFromSlot = shardEngine.extractShardIndexLambda();
  }

  public List<? extends C> getShards() {
    return Collections.unmodifiableList(Arrays.asList(shards));
  }

  public void setMutationConsumer(Consumer<BufferDataTuple> mutationConsumer) {
    getShards().forEach(shard -> shard.setMutationConsumer(mutationConsumer));
  }

  public void addChangeListener(int shardIndex, ChangeListener listener) {
    shards[shardIndex].addChangeListener(listener);
  }

  public void consumeMutationsThenRemoveListener(int shardIndex, ChangeListener listener, Consumer<Iterable<BufferDataTuple>> mutationConsumer) {
    shards[shardIndex].consumeMutationsThenRemoveListener(listener, mutationConsumer);
  }

  @Override
  public SovereignRuntime<K> runtime() {
    return runtime;
  }

  @SuppressWarnings("RedundantTypeArguments")
  @Override
  public long getPersistentBytesUsed() {
    return Stream.of(shards).mapToLong(SovereignContainer<K>::getPersistentBytesUsed).sum();
  }

  @SuppressWarnings("RedundantTypeArguments")
  @Override
  public long getAllocatedPersistentSupportStorage() {
    return Stream.of(shards).mapToLong(SovereignContainer<K>::getAllocatedPersistentSupportStorage).sum();
  }

  @SuppressWarnings("RedundantTypeArguments")
  @Override
  public long getOccupiedPersistentSupportStorage() {
    return Stream.of(shards).mapToLong(SovereignContainer<K>::getOccupiedPersistentSupportStorage).sum();
  }

  private C shardForKey(Object k) {
    int index = shardEngine.shardIndexForKey(k);
    return shards[index];
  }

  public C shardForSlot(long slot) {
    if (slot < 0) {
      throw new IllegalArgumentException();
    }
    int index = shardIndexFromSlot.transmute(slot);
    return shards[index];
  }

  private C shardForLocator(PersistentMemoryLocator loc) {
    if (loc.index() < 0) {
      return shards[0];
    }
    return shardForSlot(loc.index());
  }

  @Override
  public PersistentMemoryLocator add(SovereignPersistentRecord<K> data) {
    return shardForKey(data.getKey()).add(data);
  }

  @Override
  public PersistentMemoryLocator reinstall(long lsn, long persistentKey, ByteBuffer data) {
    return shardForSlot(persistentKey).reinstall(lsn, persistentKey, data);
  }

  @Override
  public boolean delete(PersistentMemoryLocator key) {
    return shardForLocator(key).delete(key);
  }

  @Override
  public PersistentMemoryLocator replace(PersistentMemoryLocator key, SovereignPersistentRecord<K> data) {
    return shardForLocator(key).replace(key, data);
  }

  @Override
  public SovereignPersistentRecord<K> get(PersistentMemoryLocator key) {
    if (key.isValid() && key.index() >= 0) {
      return shardForLocator(key).get(key);
    }
    return null;
  }

  @Override
  public PersistentMemoryLocator first(ContextImpl context) {

    PersistentMemoryLocator start = shards[0].first(context);
    int startShard = 0;
    if (!start.isValid()) {
      for (startShard = 1; startShard < shards.length; startShard++) {
        start = shards[startShard].first(context);
        if (start.isValid()) {
          break;
        }
      }
    }

    // all empty case
    if (!start.isValid()) {
      return start;
    }

    final int finalStartShard = startShard;
    final PersistentMemoryLocator finalStart = start;

    LocatorFactory fact = new LocatorFactory() {
      int shard = finalStartShard;
      PersistentMemoryLocator last = finalStart;

      @Override
      public Locator createNext() {
        if (!last.isValid()) {
          return last;
        }
        last = last.next();
        if (!last.isValid()) {
          for (shard++; shard < shards.length; shard++) {
            last = shards[shard].first(context);
            if (last.isValid()) {
              break;
            }
          }
        }
        if (last.isValid()) {
          return new PersistentMemoryLocator(last.index(), this);
        } else {
          return PersistentMemoryLocator.INVALID;
        }
      }

      @Override
      public Locator createPrevious() {
        return PersistentMemoryLocator.INVALID;
      }

      @Override
      public Locator.TraversalDirection direction() {
        return Locator.TraversalDirection.FORWARD;
      }
    };
    return new PersistentMemoryLocator(start.index(), fact);
  }

  @Override
  public PersistentMemoryLocator last() {
    return shards[0].last();
  }

  @Override
  public void drop() {
    for (SovereignContainer<?> c : shards) {
      c.drop();
    }
  }

  @Override
  public void dispose() {
    for (SovereignContainer<?> c : shards) {
      c.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return shards[0].isDisposed();
  }

  @Override
  public ContextImpl start(boolean guard) {
    return shards[0].start(guard);
  }

  @Override
  public void end(ContextImpl loc) {
    loc.close();
  }

  @Override
  public long count() {
    return Arrays.stream(shards).mapToLong(SovereignContainer<K>::count).sum();
  }

  @Override
  public long getUsed() {
    return Arrays.stream(shards).mapToLong(SovereignContainer<K>::getUsed).sum();
  }

  @Override
  public long getReserved() {
    return Arrays.stream(shards).mapToLong(SovereignContainer<K>::getReserved).sum();
  }

  @Override
  public PersistentMemoryLocator createLocator(long mapped, LocatorFactory locatorFactory) {
    return shards[0].createLocator(mapped, locatorFactory);
  }

  @Override
  public long mapLocator(PersistentMemoryLocator loc) {
    return shards[0].mapLocator(loc);
  }

  @Override
  public void finishRestart() {
    for (C c : shards) {
      ((PersistableDataContainer) c).finishRestart();
    }
  }

  @Override
  public void setBroker(ContainerPersistenceBroker broker) {
    for (C c : shards) {
      @SuppressWarnings("unchecked")
      PersistableDataContainer<?, ContainerPersistenceBroker> persistableDataContainer = (PersistableDataContainer<?, ContainerPersistenceBroker>) c;
      persistableDataContainer.setBroker(broker);
    }
  }
}
