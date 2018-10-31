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

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.IngestHandle;
import com.terracottatech.sovereign.btrees.bplustree.model.KeyValueHandler;
import com.terracottatech.sovereign.btrees.bplustree.model.ReadTx;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.impl.memory.storageengines.PrimitivePortability;
import com.terracottatech.sovereign.impl.memory.storageengines.SlotPrimitivePortability.KeyAndSlot;
import com.terracottatech.sovereign.impl.memory.storageengines.SlotPrimitiveStore;
import com.terracottatech.sovereign.impl.model.SovereignSecondaryIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignShardObject;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.impl.utils.batchsort.BatchParallelMergeSort;
import com.terracottatech.sovereign.impl.utils.batchsort.OffheapSortIndex;
import com.terracottatech.sovereign.impl.utils.batchsort.SortArea;
import com.terracottatech.sovereign.impl.utils.batchsort.SortScratchPad;
import com.terracottatech.sovereign.impl.utils.offheap.PowerOfTwoStore;
import com.terracottatech.sovereign.spi.store.Context;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.sovereign.spi.store.LocatorFactory;
import com.terracottatech.store.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mscott
 */
public class BtreeIndexMap<K extends Comparable<K>, RK extends Comparable<RK>> implements SovereignSortedIndexMap<K, RK>,
  SovereignSecondaryIndexMap<K, RK>, SovereignShardObject {


  public static class BtreePersistentMemoryLocator<KK> extends PersistentMemoryLocator implements SovereignShardObject {

    private final KK k;
    private final int shardIndex;

    private BtreePersistentMemoryLocator(int shardIndex) {
      super(PersistentMemoryLocator.INVALID.index(), null);
      this.shardIndex = shardIndex;
      k = null;
    }

    @Override
    public LocatorFactory factory() {
      return super.factory();
    }

    public BtreePersistentMemoryLocator(int shardIndex, KeyAndSlot<KK> ks, LocatorFactory factory) {
      this(shardIndex, ks.getSlot(), ks.getKey(), factory);
    }

    public BtreePersistentMemoryLocator(int shardIndex, long slot, KK key, LocatorFactory factory) {
      super(slot, factory);
      this.shardIndex = shardIndex;
      this.k = key;
    }

    @SuppressWarnings("unchecked")
    @Override
    public BtreePersistentMemoryLocator<KK> next() {
      return (BtreePersistentMemoryLocator<KK>) super.next();
    }

    public KK getKey() {
      return k;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      BtreePersistentMemoryLocator<?> locator = (BtreePersistentMemoryLocator<?>) o;

      return k != null ? k.equals(locator.k) : locator.k == null;

    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (k != null ? k.hashCode() : 0);
      return result;
    }

    @Override
    public int getShardIndex() {
      return shardIndex;
    }
  }

  private class Deletia {
    private final long id;

    private final long rev;

    public Deletia(long id, long rev) {
      this.id = id;
      this.rev = rev;
    }

    public long getId() {
      return id;
    }

    public long getRev() {
      return rev;
    }
  }

  static class InvalidBtreePersistentMemoryLocator<KK> extends BtreePersistentMemoryLocator<KK> {

    public InvalidBtreePersistentMemoryLocator() {
      super(-1);
    }

    @Override
    public boolean isEndpoint() {
      return true;
    }
  }

  private final ABPlusTree<TxDecorator> tree;
  private final SovereignRuntime<?> runtime;
  private final int shardIndex;
  private final SlotPrimitiveStore<K> primitiveSlotStore;
  private final PowerOfTwoStore space;
  private final AtomicLong statFirst = new AtomicLong(0);
  private final AtomicLong statLast = new AtomicLong(0);
  private final AtomicLong statGet = new AtomicLong(0);
  private final AtomicLong statHigher = new AtomicLong(0);
  private final AtomicLong statHigherEqual = new AtomicLong(0);
  private final AtomicLong statLower = new AtomicLong(0);
  private final AtomicLong statLowerEqual = new AtomicLong(0);
  private ConcurrentLinkedQueue<Deletia> toDelete = new ConcurrentLinkedQueue<>();
  private volatile boolean isDropped = false;
  private final BtreePersistentMemoryLocator<K> invalid = new InvalidBtreePersistentMemoryLocator<>();

  public BtreeIndexMap(SovereignRuntime<?> runtime, String purpose, int shardIndex,
                       Class<K> type, PageSourceLocation source) {
    this.runtime = runtime;
    this.shardIndex = shardIndex;
    Type<K> keyType = Type.forJdkType(type);
    this.primitiveSlotStore = new SlotPrimitiveStore<K>(source.getPageSource(), keyType);
    this.space = new PowerOfTwoStore(source, 4096, Math.min(1024 * 1024, runtime.getMaxResourceChunkSize()), runtime.getMaxResourceChunkSize());
    try {
      tree = new ABPlusTree<TxDecorator>(space, null, null, ABPlusTree.calculateMaxKeyCountForPageSize(4096),
                            new KeyValueHandler<TxDecorator>() {

                              @Override
                              public long compareLongKeys(Tx<TxDecorator> tx, long target, long k2) throws IOException {
                                int ret = primitiveSlotStore.compare(target, k2);
                                return ret;
                              }

                              @Override
                              public long compareObjectKeys(Tx<TxDecorator> tx, Object target, long k2) throws IOException {
                                KeyAndSlot<K> right = primitiveSlotStore.get(k2);
                                @SuppressWarnings("unchecked")
                                KeyAndSlot<K> left = (KeyAndSlot<K>) target;
                                int ret = primitiveSlotStore.compare(left, right);
                                return ret;
                              }
                            }, 0) {
        @Override
        public void notifyOfGC(long revision) {
          // this is how we snoop the gc process.
          // this means any deletions earlier than revision are fair game to be deleted.
          BtreeIndexMap.this.processGC(revision);
        }
      };
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void reentrantWriteLock() {
    tree.getTreeLock().lock();
  }

  @Override
  public void reentrantWriteUnlock() {
    tree.getTreeLock().unlock();
  }

  @SuppressWarnings("unchecked")
  PrimitivePortability<K> getKeyBasePrimitive() {
    return primitiveSlotStore.getKPrimitive();
  }

  @Override
  public int getShardIndex() {
    return shardIndex;
  }

  private void processGC(long revision) {
    Deletia p = toDelete.poll();
    while (p != null) {
      if (p.getRev() < revision) {
        primitiveSlotStore.remove(p.getId());
      } else {
        queueDeletion(p);
        break;
      }
      p = toDelete.poll();
    }
  }

  @Override
  public BtreePersistentMemoryLocator<K> put(RK recordkey, ContextImpl c, K key, PersistentMemoryLocator loc) {
    this.testDropped();
    WriteTx<TxDecorator> wt = tree.writeTx();
    try {
      wt.begin();
      long id = primitiveSlotStore.add(key, loc.index());
      wt.insert(id, loc.index());
      wt.commit();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      closeBestEffort(wt);
    }
    return invalid;
  }

  @Override
  public boolean remove(RK recordkey, ContextImpl c, K key, PersistentMemoryLocator loc) {
    this.testDropped();
    WriteTx<TxDecorator> wt = tree.writeTx();
    boolean closed = false;
    try {
      wt.begin();
      Cursor index = wt.cursor();
      // find the first one.
      boolean found = index.moveTo(primitiveSlotStore.keyAndSlot(key, loc.index()));
      if (found) {
        BtreeEntry p = index.next();
        long id = p.getKey();
        wt.delete(id);
        long rev = wt.getWorkingRevision();
        wt.commit();
        wt.close();
        // clean now
        queueDeletion(new Deletia(id, rev));
        closed = true;
        return true;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      if (!closed) {
        closeBestEffort(wt);
      }
    }
    return false;
  }

  private void queueDeletion(Deletia e) {
    toDelete.add(e);
  }

  @Override
  public BtreePersistentMemoryLocator<K> higher(ContextImpl c, K key) {
    this.testDropped();
    Tx<TxDecorator> reader = tree.readTx();
    try {
      Cursor index = reader.cursor();
      KeyAndSlot<K> ks = primitiveSlotStore.keyAndSlot(key, Long.MAX_VALUE);
      if(index.moveTo(ks)) {
        index.next();
      }
      statHigher.incrementAndGet();
      return convertToLocator(c, reader, index, false);
    } catch (IOException ioe) {
      reader.close();
      throw new RuntimeException(ioe);
    } catch (RuntimeException | Error e) {
      reader.close();
      throw e;
    }
  }

  private BtreePersistentMemoryLocator<K> rawget(ContextImpl context, K key, AtomicLong stat) {
    this.testDropped();
    Tx<TxDecorator> reader = tree.readTx();
    try {
      Cursor index = reader.cursor();
      index.moveTo(primitiveSlotStore.keyAndSlot(key, 0));
      stat.incrementAndGet();
      return convertToLocator(context, reader, index, false);
    } catch (IOException ioe) {
      reader.close();
      throw new RuntimeException(ioe);
    } catch (RuntimeException | Error e) {
      reader.close();
      throw e;
    }
  }

  @Override
  public boolean replace(RK recordKey, ContextImpl context, K oldKey, K newKey, PersistentMemoryLocator pointer) {
    this.testDropped();
    WriteTx<TxDecorator> wt = tree.writeTx();
    try {
      wt.begin();
      Cursor index = wt.cursor();
      // find the first one.
      boolean found = index.moveTo(primitiveSlotStore.keyAndSlot(oldKey, pointer.index()));
      if (found) {
        BtreeEntry p = index.next();
        long oldid = p.getKey();
        wt.delete(oldid);
        long rev = wt.getWorkingRevision();
        long newid = primitiveSlotStore.add(newKey, pointer.index());
        wt.insert(newid, pointer.index());
        wt.commit();
        // clean now
        queueDeletion(new Deletia(oldid, rev));
        return true;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      closeBestEffort(wt);
    }
    return false;
  }

  @Override
  public BtreePersistentMemoryLocator<K> get(ContextImpl context, K key) {
    return rawget(context, key, statGet);
  }

  @Override
  public BtreePersistentMemoryLocator<K> higherEqual(ContextImpl context, K key) {
    return rawget(context, key, statHigherEqual);
  }

  @Override
  public BtreePersistentMemoryLocator<K> lower(ContextImpl c, K key) {
    this.testDropped();
    Tx<TxDecorator> reader = tree.readTx();
    try {
      Cursor index = reader.cursor();
      index.moveTo(primitiveSlotStore.keyAndSlot(key, 0));
      statLower.incrementAndGet();
      return convertToLocator(c, reader, index, true);
    } catch (IOException ioe) {
      reader.close();
      throw new RuntimeException(ioe);
    } catch (RuntimeException | Error e) {
      reader.close();
      throw e;
    }
  }

  @Override
  public BtreePersistentMemoryLocator<K> lowerEqual(ContextImpl c, K key) {
    this.testDropped();
    Tx<TxDecorator> reader = tree.readTx();
    try {
      Cursor index = reader.cursor();
      if(index.moveTo(primitiveSlotStore.keyAndSlot(key, Long.MAX_VALUE))) {
        index.next();
      }
      statLowerEqual.incrementAndGet();
      return convertToLocator(c, reader, index, true);
    } catch (IOException ioe) {
      reader.close();
      throw new RuntimeException(ioe);
    } catch (RuntimeException | Error e) {
      reader.close();
      throw e;
    }
  }

  @Override
  public BtreePersistentMemoryLocator<K> first(ContextImpl c) {
    this.testDropped();
    Tx<TxDecorator> reader = tree.readTx();
    try {
      Cursor index = reader.cursor();
      index.first();
      statFirst.incrementAndGet();
      return convertToLocator(c, reader, index, false);
    } catch (IOException ioe) {
      reader.close();
      throw new RuntimeException(ioe);
    } catch (RuntimeException | Error e) {
      reader.close();
      throw e;
    }
  }

  @Override
  public BtreePersistentMemoryLocator<K> last(ContextImpl c) {
    this.testDropped();
    Tx<TxDecorator> reader = tree.readTx();
    try {
      Cursor index = reader.cursor();
      index.last();
      statLast.incrementAndGet();
      return convertToLocator(c, reader, index, true);
    } catch (IOException ioe) {
      reader.close();
      throw new RuntimeException(ioe);
    } catch (RuntimeException | Error e) {
      reader.close();
      throw e;
    }
  }

  private void closeBestEffort(WriteTx<TxDecorator> wt) {
    try {
      wt.discard();
    } catch (IOException e) {
      // TODO log here
    }
    wt.close();
  }

  @Override
  public void drop() {
    if (this.isDropped) {
      return;
    }
    this.isDropped = true;
    toDelete.clear();
    this.primitiveSlotStore.dispose();
    try {
      this.tree.close();
    } catch (IOException e) {
    }
    try {
      this.space.close();
    } catch (Throwable e) {
    }
  }

  private void testDropped() throws IllegalStateException {
    if (this.isDropped) {
      throw new IllegalStateException("Attempt to use dropped BtreeIndexMap");
    }
  }

  @Override
  public long estimateSize() {
    this.testDropped();
    return tree.getSnapShot().getSize();
  }

  private BtreePersistentMemoryLocator<K> convertToLocator(Context cxt, Tx<TxDecorator> reader, Cursor c, boolean back) {
    Locator.TraversalDirection dir = back ? Locator.TraversalDirection.REVERSE : Locator.TraversalDirection.FORWARD;
    BtreeCursorLocatorFactory factory = new BtreeCursorLocatorFactory(cxt, dir, reader, c);
    cxt.addCloseable(factory);
    return (back) ? factory.createPrevious() : factory.createNext();
  }

  /**
   * Obtains the key corresponding to the uid provided.
   *
   * @param uid the uid for which the key is to be obtained
   * @return the non-null key for {@code uid}
   * @throws KeyExtractException if uid has no key mapping.  In general, this indicates that the transaction
   * need to be retried.
   */
  private KeyAndSlot<K> extractKey(long uid) {
    this.testDropped();
    KeyAndSlot<K> ks = primitiveSlotStore.get(uid);
    if (ks == null) {
      throw new KeyExtractException(uid);
    }
    return ks;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("BtreeIndexMap{");
    sb.append("dropped=").append(this.isDropped);
    if (!this.isDropped) {
      try (ReadTx<TxDecorator> reader = tree.readTx()) {
        sb.append(", size=").append(tree.size(reader));
      }
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public SovereignRuntime<?> runtime() {
    return runtime;
  }

  private class BtreeCursorLocatorFactory implements MemoryLocatorFactory, AutoCloseable {
    private final Context ctx;      // Held for liveliness
    private final Tx<TxDecorator> transaction;
    private final Cursor position;
    private final Locator.TraversalDirection direction;

    public BtreeCursorLocatorFactory(Context ctx, Locator.TraversalDirection direction, Tx<TxDecorator> transaction, Cursor position) {
      this.ctx = ctx;
      this.transaction = transaction;
      this.position = position;
      this.direction = direction;
    }

    @Override
    public void close() {
      transaction.close();
    }

    @Override
    public BtreePersistentMemoryLocator<K> createPrevious() {
      BtreeIndexMap.this.testDropped();
      if (direction.isReverse()) {
        if (position.hasPrevious()) {
          BtreeEntry p = position.previous();
          KeyAndSlot<K> ks = primitiveSlotStore.get(p.getKey());
          return new BtreePersistentMemoryLocator<>(BtreeIndexMap.this.getShardIndex(), ks, this);
        }
      }
      return invalid;
    }

    @Override
    public Locator.TraversalDirection direction() {
      return direction;
    }

    @Override
    public BtreePersistentMemoryLocator<K> createNext() {
      BtreeIndexMap.this.testDropped();
      if (direction.isForward()) {
        if (position.hasNext()) {
          BtreeEntry p = position.next();
          KeyAndSlot<K> ks = primitiveSlotStore.get(p.getKey());
          return new BtreePersistentMemoryLocator<>(BtreeIndexMap.this.getShardIndex(), ks, this);
        }
      }
      return invalid;
    }

  }

  /**
   * Thrown from {@link #extractKey(long)} to indicate the key was not found for
   * the {@code uid}.
   */
  private static class KeyExtractException extends RuntimeException {
    private static final long serialVersionUID = 8858442268627566436L;
    final long uid;

    public KeyExtractException(final long uid) {
      super(Long.toHexString(uid));
      this.uid = uid;
    }

    public long getUid() {
      return this.uid;
    }
  }

  @Override
  public long statLookupFirstCount() {
    return statFirst.get();
  }

  @Override
  public long statLookupLastCount() {
    return statLast.get();
  }

  @Override
  public long statLookupEqualCount() {
    return statGet.get();
  }

  @Override
  public long statLookupHigherCount() {
    return statHigher.get();
  }

  @Override
  public long statLookupHigherEqualCount() {
    return statHigherEqual.get();
  }

  @Override
  public long statLookupLowerCount() {
    return statLower.get();
  }

  @Override
  public long statLookupLowerEqualCount() {
    return statLowerEqual.get();
  }

  @Override
  public long getOccupiedStorageSize() {
    try {
      return primitiveSlotStore.getOccupiedBytes() + space.getStats().getUserBytes();
    } catch (Throwable t) {
    }
    return 0l;
  }

  @Override
  public long getAllocatedStorageSize() {
    try {
      return primitiveSlotStore.getAllocatedBytes() + space.getStats().getAllocatedBytes();
    } catch (Throwable t) {
    }
    return 0l;
  }

  @Override
  public long statAccessCount() {
    return statLowerEqual.get() + statLower.get() + statHigher.get() + statHigherEqual.get() + statFirst.get() + statLast
      .get() + statGet.get();
  }

  private Comparator<ByteBuffer> ingestComparator = new Comparator<ByteBuffer>() {
    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
      long leftId = NIOBufferUtils.bufferToLong(o1);
      long rightId = NIOBufferUtils.bufferToLong(o2);
      int ret = primitiveSlotStore.compare(leftId, rightId);
      return ret;
    }
  };

  @Override
  public BatchHandle<K> batch() {
    WriteTx<TxDecorator> wt = tree.writeTx();
    wt.begin();
    return new BatchHandle<K>() {
      SortArea<KeyAndSlot<K>> area = SortArea.areaOf(new OffheapSortIndex(4096), new SortScratchPad<KeyAndSlot<K>>() {
        @Override
        public long ingest(ByteBuffer k, KeyAndSlot<K> v) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public AbstractMap.Entry<ByteBuffer, KeyAndSlot<K>> fetchKV(long addr) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer fetchK(long addr) throws IOException {
          return NIOBufferUtils.longToBuffer(addr);
        }

        @Override
        public long count() {
          return area.getIndex().size();
        }

        @Override
        public void clear() throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public void dispose() throws IOException {
        }
      });

      @Override
      public void batchAdd(Object recordKey, K key, PersistentMemoryLocator loc) {
        BtreeIndexMap.this.testDropped();
        long id = primitiveSlotStore.add(key, loc.index());
        area.getIndex().add(id);
      }

      @Override
      public void process() throws IOException {
        try {
          BatchParallelMergeSort sort = new BatchParallelMergeSort();
          sort.sort(area, ingestComparator);
          IngestHandle h = tree.startBatch(wt);
          for (long l = 0; l < area.size(); l++) {
            long v = area.getIndex().addressOf(l);
            KeyAndSlot<K> ks = primitiveSlotStore.get(v);
            h.ingest(v, ks.getSlot());
          }
          h.endBatch();
        } catch (Exception e) {
          throw new IOException(e);
        } finally {
          area.dispose();
        }
      }

      @Override
      public void close() throws IOException {
        wt.commit();
        closeBestEffort(wt);
      }
    };
  }

}
