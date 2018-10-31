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

import com.terracottatech.sovereign.impl.memory.BtreeIndexMap.BtreePersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.storageengines.PrimitivePortability;
import com.terracottatech.sovereign.impl.model.SovereignSecondaryIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.sovereign.spi.store.LocatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * @author mscott
 */
public class ShardedBtreeIndexMap<K extends Comparable<K>, RK extends Comparable<RK>> implements SovereignSortedIndexMap<K, RK>,
  SovereignSecondaryIndexMap<K, RK> {


  enum Op {
    GET,
    HIGHER,
    HIGHEREQ,
    LOWER,
    LOWEREQ,
    FIRST,
    LAST
  }

  private final PrimitivePortability<K> primitiveBasePortability;
  private final BtreeIndexMap<K, RK>[] shards;
  private final SovereignRuntime<?> runtime;
  private final KeySlotShardEngine shardEngine;
  private final BtreePersistentMemoryLocator<K> invalid = new BtreeIndexMap.InvalidBtreePersistentMemoryLocator<>();

  public ShardedBtreeIndexMap(BtreeIndexMap<K, RK>[] shards) {
    if (Integer.bitCount(shards.length) != 1) {
      throw new IllegalArgumentException("Shard length must be power of 2");
    }
    this.shards = Arrays.copyOf(shards, shards.length);
    this.runtime = shards[0].runtime();
    this.shardEngine = runtime.getShardEngine();
    this.primitiveBasePortability = shards[0].getKeyBasePrimitive();
  }

  private int shardIndexFor(Object k) {
    return shardEngine.shardIndexForKey(k);
  }

  private BtreeIndexMap<K, RK> shardFor(Object k) {
    int index = shardEngine.shardIndexForKey(k);
    return shards[index];
  }

  public List<BtreeIndexMap<K, RK>> getShards() {
    return Collections.unmodifiableList(Arrays.asList(shards));
  }

  @Override
  public void reentrantWriteLock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reentrantWriteUnlock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean replace(RK recordKey, ContextImpl context, K oldKey, K newKey, PersistentMemoryLocator pointer) {
    BtreeIndexMap<K, RK> shard = shardFor(recordKey);
    return shard.replace(recordKey, context, oldKey, newKey, pointer);
  }

  // core mutation methods
  public BtreePersistentMemoryLocator<K> get(ContextImpl c, K key) {
    ShardedLocaterFactory fact = new ShardedLocaterFactory(Op.GET, c, key);
    return fact.goNext();
  }

  public PersistentMemoryLocator put(RK recordKey, ContextImpl c, K key, PersistentMemoryLocator loc) {
    return shardFor(recordKey).put(recordKey, c, key, loc);
  }

  public boolean remove(RK recordKey, ContextImpl c, K key, PersistentMemoryLocator loc) {
    return shardFor(recordKey).remove(recordKey, c, key, loc);
  }

  // traversal methods
  public BtreePersistentMemoryLocator<K> higherEqual(ContextImpl c, K key) {
    ShardedLocaterFactory fact = new ShardedLocaterFactory(Op.HIGHEREQ, c, key);
    return fact.goNext();
  }

  public BtreePersistentMemoryLocator<K> higher(ContextImpl c, K key) {
    ShardedLocaterFactory fact = new ShardedLocaterFactory(Op.HIGHER, c, key);
    return fact.goNext();
  }

  public BtreePersistentMemoryLocator<K> lower(ContextImpl c, K key) {
    ShardedLocaterFactory fact = new ShardedLocaterFactory(Op.LOWER, c, key);
    return fact.goNext();
  }

  public BtreePersistentMemoryLocator<K> lowerEqual(ContextImpl c, K key) {
    ShardedLocaterFactory fact = new ShardedLocaterFactory(Op.LOWEREQ, c, key);
    return fact.goNext();
  }

  public BtreePersistentMemoryLocator<K> first(ContextImpl c) {
    ShardedLocaterFactory fact = new ShardedLocaterFactory(Op.FIRST, c, null);
    return fact.goNext();
  }

  public BtreePersistentMemoryLocator<K> last(ContextImpl c) {
    ShardedLocaterFactory fact = new ShardedLocaterFactory(Op.LAST, c, null);
    return fact.goNext();
  }

  public void drop() {
    for (BtreeIndexMap<K, RK> m : shards) {
      m.drop();
    }
  }

  public long estimateSize() {
    return Arrays.asList(shards).stream().mapToLong(BtreeIndexMap::estimateSize).sum();
  }

  public SovereignRuntime<?> runtime() {
    return runtime;
  }

  private class ShardedLocaterFactory implements LocatorFactory {
    private PriorityQueue<BtreePersistentMemoryLocator<K>> q;
    private Locator.TraversalDirection dir;

    public ShardedLocaterFactory(Op op, ContextImpl c, K key) {
      initQ(op, c, key);
    }

    public ShardedBtreeIndexMap<K, RK> parent() {
      return ShardedBtreeIndexMap.this;
    }

    @SuppressWarnings("unchecked")
    private int forwardCompare(Object o1, Object o2) {
      BtreePersistentMemoryLocator<K> l = (BtreePersistentMemoryLocator<K>) o1;
      BtreePersistentMemoryLocator<K> r = (BtreePersistentMemoryLocator<K>) o2;
      int ret = primitiveBasePortability.compare(l.getKey(), r.getKey());
      if (ret == 0) {
        ret = Integer.compare(l.getShardIndex(), r.getShardIndex());
        if (ret == 0) {
          ret = Long.compare(l.index(), r.index());
        }
      }
      return ret;
    }

    private void initQ(Op op, ContextImpl c, K key) {
      switch (op) {
        case GET:
          dir = Locator.TraversalDirection.FORWARD;
          q = new PriorityQueue<>((Comparator<Object>) this::forwardCompare);
          for (BtreeIndexMap<K, RK> s : shards) {
            BtreePersistentMemoryLocator<K> pl = s.get(c, key);
            if (pl.isValid()) {
              q.add(pl);
            }
          }
          break;
        case HIGHEREQ:
          dir = Locator.TraversalDirection.FORWARD;
          q = new PriorityQueue<>((Comparator<Object>) this::forwardCompare);
          for (BtreeIndexMap<K, RK> s : shards) {
            BtreePersistentMemoryLocator<K> pl = s.higherEqual(c, key);
            if (pl.isValid()) {
              q.add(pl);
            }
          }
          break;
        case HIGHER:
          dir = Locator.TraversalDirection.FORWARD;
          q = new PriorityQueue<>((Comparator<Object>) this::forwardCompare);
          for (BtreeIndexMap<K, RK> s : shards) {
            BtreePersistentMemoryLocator<K> pl = s.higher(c, key);
            if (pl.isValid()) {
              q.add(pl);
            }
          }
          break;
        case LOWER:
          dir = Locator.TraversalDirection.REVERSE;
          q = new PriorityQueue<>((Comparator<Object>) (o1, o2) -> 0 - forwardCompare(o1, o2));
          for (BtreeIndexMap<K, RK> s : shards) {
            BtreePersistentMemoryLocator<K> pl = s.lower(c, key);
            if (pl.isValid()) {
              q.add(pl);
            }
          }
          break;
        case LOWEREQ:
          dir = Locator.TraversalDirection.REVERSE;
          q = new PriorityQueue<>((Comparator<Object>) (o1, o2) -> 0 - forwardCompare(o1, o2));
          for (BtreeIndexMap<K, RK> s : shards) {
            BtreePersistentMemoryLocator<K> pl = s.lowerEqual(c, key);
            if (pl.isValid()) {
              q.add(pl);
            }
          }
          break;
        case FIRST:
          dir = Locator.TraversalDirection.FORWARD;
          q = new PriorityQueue<>((Comparator<Object>) this::forwardCompare);
          for (BtreeIndexMap<K, RK> s : shards) {
            BtreePersistentMemoryLocator<K> pl = s.first(c);
            if (pl.isValid()) {
              q.add(pl);
            }
          }
          break;
        case LAST:
          dir = Locator.TraversalDirection.REVERSE;
          q = new PriorityQueue<>((Comparator<Object>) (o1, o2) -> 0 - forwardCompare(o1, o2));
          for (BtreeIndexMap<K, RK> s : shards) {
            BtreePersistentMemoryLocator<K> pl = s.last(c);
            if (pl.isValid()) {
              q.add(pl);
            }
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }

    @Override
    public BtreePersistentMemoryLocator<K> createNext() {
      if (dir == Locator.TraversalDirection.FORWARD) {
        BtreePersistentMemoryLocator<K> ret = goNext();
        return ret;
      }
      return invalid;
    }

    @Override
    public BtreePersistentMemoryLocator<K> createPrevious() {
      if (dir == Locator.TraversalDirection.REVERSE) {
        BtreePersistentMemoryLocator<K> ret = goNext();
        return ret;
      }
      return invalid;
    }

    private BtreePersistentMemoryLocator<K> goNext() {
      BtreePersistentMemoryLocator<K> ret = q.poll();
      if (ret != null && ret.isValid()) {
        BtreePersistentMemoryLocator<K> p = ret.next();
        if (p.isValid()) {
          q.add(p);
        }
        return new BtreePersistentMemoryLocator<>(ret.getShardIndex(), ret.index(), ret.getKey(), this);
      }
      return invalid;
    }

    @Override
    public Locator.TraversalDirection direction() {
      return dir;
    }
  }

  public static SovereignSortedIndexMap<?, ?> internalBtreeFor(Locator loc) {
    if (loc instanceof BtreePersistentMemoryLocator) {
      BtreePersistentMemoryLocator<?> tloc = (BtreePersistentMemoryLocator<?>) loc;
      LocatorFactory fact = tloc.factory();
      if (fact instanceof ShardedBtreeIndexMap.ShardedLocaterFactory) {
        ShardedBtreeIndexMap<?, ?>.ShardedLocaterFactory shardFact = (ShardedBtreeIndexMap<?, ?>.ShardedLocaterFactory) fact;
        return shardFact.parent();
      }
    }
    return null;
  }

  @SuppressWarnings("rawtype")
  private long sumOverShards(ToLongFunction<BtreeIndexMap<?, ?>> sizeFunction) {
    return Stream.of(shards).mapToLong(sizeFunction).sum();
  }

  @Override
  public long getOccupiedStorageSize() {
    return sumOverShards(SovereignSortedIndexMap::getOccupiedStorageSize);
  }

  @Override
  public long getAllocatedStorageSize() {
    return sumOverShards(SovereignSortedIndexMap::getAllocatedStorageSize);
  }

  @Override
  public long statAccessCount() {
    return sumOverShards(SovereignSortedIndexMap::statAccessCount);
  }

  @Override
  public long statLookupFirstCount() {
    return sumOverShards(SovereignSortedIndexMap::statLookupFirstCount);
  }

  @Override
  public long statLookupLastCount() {
    return sumOverShards(SovereignSortedIndexMap::statLookupLastCount);
  }

  @Override
  public long statLookupEqualCount() {
    return sumOverShards(SovereignSortedIndexMap::statLookupEqualCount);
  }

  @Override
  public long statLookupHigherCount() {
    return sumOverShards(SovereignSortedIndexMap::statLookupHigherCount);
  }

  @Override
  public long statLookupHigherEqualCount() {
    return sumOverShards(SovereignSortedIndexMap::statLookupHigherEqualCount);
  }

  @Override
  public long statLookupLowerCount() {
    return sumOverShards(SovereignSortedIndexMap::statLookupLowerCount);
  }

  @Override
  public long statLookupLowerEqualCount() {
    return sumOverShards(SovereignSortedIndexMap::statLookupLowerEqualCount);
  }

  @Override
  public BatchHandle<K> batch() {
    @SuppressWarnings({"unchecked", "rawtypes"})
    BatchHandle<K>[] handles = new BatchHandle[shards.length];
    for (int i = 0; i < handles.length; i++) {
      handles[i] = shards[i].batch();
    }
    return new BatchHandle<K>() {
      AtomicReference<IOException> exc = new AtomicReference<>();

      @Override
      public void batchAdd(Object recordKey, K key, PersistentMemoryLocator pointer) {
        int index = shardIndexFor(recordKey);
        handles[index].batchAdd(recordKey, key, pointer);
      }

      @Override
      public void process() throws IOException {
        ForkJoinPool pool = new ForkJoinPool();
        for (int i = 0; i < handles.length; i++) {
          final int finalI = i;
          ForkJoinTask<?> t = pool.submit(() -> {
            try {
              handles[finalI].process();
            } catch (IOException e) {
              exc.set(e);
            }
          });
        }
        pool.shutdown();
        try {
          pool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
        }
      }

      @Override
      public void close() throws IOException {
        try {
          for (int i = 0; i < handles.length; i++) {
            handles[i].close();
          }
        } finally {
          if (exc.get() != null) {
            throw exc.get();
          }
        }
      }
    };
  }
}
