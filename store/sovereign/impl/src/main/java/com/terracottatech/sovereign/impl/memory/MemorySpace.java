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

import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.impl.SovereignAllocationResource;
import com.terracottatech.sovereign.impl.model.SovereignIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignSpace;
import com.terracottatech.sovereign.spi.IndexMapConfig;
import com.terracottatech.sovereign.spi.store.DataContainer;
import com.terracottatech.sovereign.spi.store.IndexMap;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author mscott
 */
public class MemorySpace implements SovereignSpace {

  private final long size;
  private final int maxChunkSize;
  protected final BiFunction<?, ?, VersionLimitStrategy.Retention> versionLimitFunction;
  protected final TimeReferenceGenerator<?> timeReferenceGenerator;
  protected final RecordBufferStrategy<?> bufferStrategy;
  protected List<DataContainer<?, ?, ?>> containers = new ArrayList<>();
  private final Set<SovereignIndexMap<?, ?>> maps = Collections.newSetFromMap(new IdentityHashMap<>());
  private final SovereignRuntime<?> runtime;
  private volatile boolean isDropped = false;

  @SuppressWarnings("unchecked")
  public MemorySpace(SovereignRuntime<?> runtime) {

    this.runtime = runtime;
    this.bufferStrategy = runtime.getBufferStrategy();
    this.size = runtime.getResourceSize();
    this.maxChunkSize = runtime.getMaxResourceChunkSize();
    this.versionLimitFunction = runtime.getRecordRetrievalFilter();
    this.timeReferenceGenerator = runtime.getTimeReferenceGenerator();
  }

  @Override
  public synchronized long getCapacity() {
    return size;
  }

  @Override
  public synchronized long getUsed() {
    testDropped();
    long capacity = containers.stream().mapToLong(DataContainer::getUsed).sum();
    for (SovereignIndexMap<?, ?> m : maps) {
      if (m instanceof HashIndexMap) {
        capacity += ((HashIndexMap<?>) m).getUsed();
      }
    }
    return capacity;
  }

  @Override
  public synchronized long getReserved() {
    testDropped();
    long capacity = containers.stream().mapToLong(DataContainer::getReserved).sum();
    for (SovereignIndexMap<?, ?> m : maps) {
      if (m instanceof HashIndexMap) {
        capacity += ((HashIndexMap<?>) m).getReserved();
      }
    }
    return capacity;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("MemorySpace{");
    sb.append("isDropped=").append(isDropped);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public synchronized void drop() {
    if (this.isDropped) {
      return;
    }
    this.isDropped = true;

    this.maps.forEach(IndexMap::drop);
    this.maps.clear();

    containers.forEach((c) -> {
      c.dispose();
    });
    containers.clear();
    containers = null;
  }

  @Override
  public boolean isDropped() {
    return this.isDropped;
  }

  @Override
  public SovereignRuntime<?> runtime() {
    return runtime;
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  protected ShardedRecordContainer<?, ?> shard(Function<ShardSpec, AbstractRecordContainer<?>> supplier) {
    testDropped();
    int shardCnt = runtime.getConfig().getConcurrency();
    AbstractRecordContainer<?>[] shards = new AbstractRecordContainer[shardCnt];
    for (int i = 0; i < shardCnt; i++) {
      ShardSpec shardSpec = new ShardSpec(shardCnt, i);
      shards[i] = supplier.apply(shardSpec);
    }
    ShardedRecordContainer<?, ?> cont = new ShardedRecordContainer(shards);
    containers.add(cont);
    return cont;
  }

  /**
   * base data container.  all data associated with a dataset goes here.
   * @return base container
   */
  public synchronized ShardedRecordContainer<?, ?> createContainer() {
    testDropped();
    ShardedRecordContainer<?, ?> cont = shard(shardSpec -> {
      SovereignRuntime<?> runtime = runtime();
      SovereignAllocationResource.PageSourceAllocator source = this.runtime.allocator()
              .getNamedPageSourceAllocator(SovereignAllocationResource.Type.RecordContainer);
      return new MemoryRecordContainer<>(shardSpec, runtime, source);
    });
    containers.add(cont);
    return cont;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public synchronized <K extends Comparable<K>> SovereignIndexMap<K, ?> createMap(String purpose, IndexMapConfig<K> config) {
    testDropped();
    SovereignIndexMap<K, ?> map;
    if (config.isSortedMap()) {
      SovereignAllocationResource.PageSourceAllocator ps = runtime.allocator().getNamedPageSourceAllocator(
        SovereignAllocationResource.Type.SortedMap);
      BtreeIndexMap<K, ?>[] shards = (BtreeIndexMap<K, ?>[]) new BtreeIndexMap[runtime.getConfig().getConcurrency()];
      try {
        for (int i = 0; i < shards.length; i++) {
          BtreeIndexMap<K, ?> bm = new BtreeIndexMap<>(runtime(),
                                                    purpose,
                                                    i,
                                                    config.getType().getJDKType(),
                                                    new PageSourceLocation(ps, size, maxChunkSize)
          );
          shards[i] = bm;
        }
      } catch (RuntimeException e) {
        for (BtreeIndexMap<K, ?> shard : shards) {
          if (shard != null) {
            shard.drop();
          }
        }
        throw e;
      }
      map = new ShardedBtreeIndexMap(shards);
    } else {
      SovereignAllocationResource.PageSourceAllocator ps = runtime.allocator().getNamedPageSourceAllocator(
        SovereignAllocationResource.Type.UnsortedMap);
      HashIndexMap<K>[] shards = new HashIndexMap[runtime.getConfig().getConcurrency()];
      try {
        for (int i = 0; i < shards.length; i++) {
          HashIndexMap<K> hm = new HashIndexMap(runtime(), i, config.getType().getJDKType(), ps);
          shards[i] = hm;
        }
      } catch (RuntimeException e) {
        for (HashIndexMap<K> shard : shards) {
          if (shard != null) {
            shard.drop();
          }
        }
        throw  e;
      }
      map = new ShardedPrimaryIndexMap(shards);
    }
    maps.add(map);
    return map;
  }

  @Override
  public synchronized <K extends Comparable<K>, RK extends Comparable<RK>> void removeMap(
    IndexMap<K, RK, ContextImpl, PersistentMemoryLocator> indexMap) {
    // May be called during drop processing; does not check if this Space is dropped
    if (this.maps != null) {
      this.maps.remove(indexMap);
      indexMap.drop();
    }
  }

  protected void testDropped() throws IllegalStateException {
    if (this.isDropped) {
      throw new IllegalStateException("Attempt to use dropped MemorySpace instance");
    }
  }
}
