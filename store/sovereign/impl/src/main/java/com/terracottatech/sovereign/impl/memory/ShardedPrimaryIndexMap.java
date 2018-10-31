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

import com.terracottatech.sovereign.impl.model.SovereignPrimaryMap;

import java.util.Arrays;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * @author cschanck
 **/
public class ShardedPrimaryIndexMap<K extends Comparable<K>> implements SovereignPrimaryMap<K> {

  private final SovereignRuntime<?> runtime;
  private final SovereignPrimaryMap<K>[] shards;
  private final KeySlotShardEngine shardEngine;

  public ShardedPrimaryIndexMap(SovereignPrimaryMap<K>[] shards) {
    this.runtime = shards[0].runtime();
    this.shards = Arrays.copyOf(shards, shards.length);
    this.shardEngine = runtime.getShardEngine();
  }

  private SovereignPrimaryMap<K> shardFor(Object k) {
    int index = shardEngine.shardIndexForKey(k);
    return shards[index];
  }

  @Override
  public void reinstall(Object key, PersistentMemoryLocator pointer) {
    shardFor(key).reinstall(key, pointer);
  }

  @Override
  public SovereignRuntime<?> runtime() {
    return runtime;
  }

  @SuppressWarnings("rawtypes")
  private long sumOverShards(ToLongFunction<SovereignPrimaryMap<?>> sizeFunction) {
    return Stream.of(shards).mapToLong(sizeFunction).sum();
  }

  @Override
  public long getOccupiedStorageSize() {
    return sumOverShards(SovereignPrimaryMap::getOccupiedStorageSize);
  }

  @Override
  public long getAllocatedStorageSize() {
    return sumOverShards(SovereignPrimaryMap::getAllocatedStorageSize);
  }

  @Override
  public long statAccessCount() {
    return sumOverShards(SovereignPrimaryMap::statAccessCount);
  }

  @Override
  public PersistentMemoryLocator get(ContextImpl context, K key) {
    return shardFor(key).get(context, key);
  }

  @Override
  public PersistentMemoryLocator put(ContextImpl context, K key, PersistentMemoryLocator pointer) {
    return shardFor(key).put(context, key, pointer);
  }

  @Override
  public boolean remove(ContextImpl context, K key, PersistentMemoryLocator pointer) {
    return shardFor(key).remove(context, key, pointer);
  }

  @Override
  public void drop() {
    for (SovereignPrimaryMap<K> hs : shards) {
      hs.drop();
    }
  }

  @Override
  public long estimateSize() {
    return Arrays.asList(shards).stream().mapToLong(SovereignPrimaryMap::estimateSize).sum();
  }

}
