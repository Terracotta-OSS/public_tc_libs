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
package com.terracottatech.sovereign.impl.persistence.base;

import com.terracottatech.frs.Disposable;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ObjectManagerStripe;
import com.terracottatech.frs.object.RestartableObject;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.impl.SovereignAllocationResource;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.AbstractRecordContainer;
import com.terracottatech.sovereign.impl.memory.ShardedRecordContainer;
import com.terracottatech.sovereign.impl.model.PersistableDataContainer;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.utils.LockSet;
import com.terracottatech.sovereign.spi.store.ContainerPersistenceBroker;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.terracottatech.sovereign.exceptions.SovereignExtinctionException.ExtinctionType;

public abstract class SovereignRestartableBroker<C extends PersistableDataContainer<?, ?>>
    implements RestartableObject<ByteBuffer, ByteBuffer, ByteBuffer>, ContainerPersistenceBroker {

  private static final long ENCODED_KEY_SIZE = 8L;

  protected final ShardedRecordContainer<?, ?> dataContainer;
  private final LockSet lockset;
  private final ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> objectManagerStripe = new MapObjectManagerStripe();
  private final RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability;
  private final ByteBuffer identifier;
  private final long identifierSize;
  private final SovereignDatasetImpl<?> dataset;
  private final AbstractPersistentStorage storage;
  private OffheapAddressToLSNMap lsnMap;
  private AtomicLong byteSize = new AtomicLong(0);
  private volatile boolean localDisposed = false;

  public SovereignRestartableBroker(ByteBuffer identifier,
                                    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability,
                                    SovereignDatasetImpl<?> dataset) {
    this.identifier = identifier;
    this.identifierSize = identifier.remaining();
    this.restartability = restartability;
    this.storage= (AbstractPersistentStorage) dataset.getStorage();
    this.dataContainer = dataset.getContainer();
    this.dataset = dataset;
    this.lockset = new LockSet(dataset.getRuntime().getShardEngine().getShardCount(), true);
    final SovereignAllocationResource.PageSourceAllocator ps = dataset.getRuntime()
        .allocator()
        .getNamedPageSourceAllocator(SovereignAllocationResource.Type.FRSAddressMap);
    lsnMap = new OffheapAddressToLSNMap(dataset.getRuntime(), ps, Long.MAX_VALUE);
    dataContainer.setBroker(this);
  }

  public static ByteBuffer uuidToBuffer(UUID uuid) {
    ByteBuffer b = ByteBuffer.allocate(16);
    b.putLong(0, uuid.getMostSignificantBits());
    b.putLong(8, uuid.getLeastSignificantBits());
    return b;
  }

  public static UUID bufferToUUID(ByteBuffer buf) {
    return new UUID(buf.getLong(buf.position()), buf.getLong(buf.position() + 8));
  }

  public void close() {
    // set the disposed flag under lock
    // so that compaction thread do not do compacting in the middle of a dispose
    lockset.lockAll();
    try {
      localDisposed = true;
    } finally {
      lockset.unlockAll();
    }
  }

  public long getByteSize() {
    return objectManagerStripe.sizeInBytes();
  }

  public RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> getRestartability() {
    return restartability;
  }

  public ByteBuffer getIdentifier() {
    return identifier;
  }

  public ByteBuffer getId() {
    return identifier;
  }

  public ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> getObjectManagerStripe() {
    return objectManagerStripe;
  }

  void rawPut(Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx,
              Long key,
              ByteBuffer value) throws TransactionException {
    ByteBuffer encodedKey = encodeKey(key);
    byteSize.addAndGet(identifierSize + ENCODED_KEY_SIZE + valueByteSize(value));
    tx.put(identifier, encodedKey, value);
  }

  void rawReplace(Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx,
                  Long key,
                  int oldValueSize,
                  ByteBuffer value) throws TransactionException {
    ByteBuffer encodedKey = encodeKey(key);
    byteSize.addAndGet(valueByteSize(value) - oldValueSize);
    tx.put(identifier, encodedKey, value);
  }

  void rawRemove(Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx,
                 Long key,
                 int oldValueSize) throws TransactionException {
    long vsize = oldValueSize;
    byteSize.addAndGet(-(8 + vsize));
    tx.remove(identifier, encodeKey(key));
  }

  void replayPut(long lsn, Long key, ByteBuffer value) {
    dataset.getPrimary().reinstall(dataset.getRuntime().getBufferStrategy().readKey(value),
        dataContainer.reinstall(lsn, key.longValue(), value));
  }

  ByteBuffer encodeKey(Long key) {
    ByteBuffer b = ByteBuffer.allocate(8);
    b.putLong(0, key);
    return b;
  }

  Long decodeKey(ByteBuffer rKey) {
    return rKey.asLongBuffer().get();
  }

  long valueByteSize(ByteBuffer encodedValue) {
    return encodedValue.remaining();
  }

  @Override
  public void tapAdd(long key, ByteBuffer data) {
    ReentrantLock l = lockset.lockFor(key);
    l.lock();
    try {
      Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx = getAutoTransaction();
      rawPut(tx, key, data);
    } catch (TransactionException e) {
      throw ExtinctionType.PERSISTENCE_ADD_FAILURE.exception(e);
    } finally {
      l.unlock();
    }
    storage.markDirty();
  }

  private Transaction<ByteBuffer, ByteBuffer, ByteBuffer> getAutoTransaction() {
    return restartability.beginAutoCommitTransaction(false);
  }

  private Transaction<ByteBuffer, ByteBuffer, ByteBuffer> getTransaction() {
    return restartability.beginTransaction(false);
  }

  private void finishOpenTransaction(Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx) throws TransactionException {
    tx.commit();
  }

  @Override
  public void tapDelete(long key, int oldDataSize) {
    ReentrantLock l = lockset.lockFor(key);
    l.lock();
    try {
      Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx = getAutoTransaction();
      rawRemove(tx, key, oldDataSize);
    } catch (TransactionException e) {
      throw ExtinctionType.PERSISTENCE_DELETE_FAILURE.exception(e);
    } finally {
      l.unlock();
    }
    storage.markDirty();
  }

  @Override
  public void tapReplace(long oldKey, int oldDataSize, long key, ByteBuffer data) {
    try {
      if (oldKey == key) {
        ReentrantLock l = lockset.lockFor(key);
        l.lock();
        try {
          Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx = getAutoTransaction();
          rawReplace(tx, key, oldDataSize, data);
        } finally {
          l.unlock();
        }
      } else {
        ReentrantLock l1;
        ReentrantLock l2;
        // deterministic ordering
        if (key < oldKey) {
          l1 = lockset.lockFor(key);
          l2 = lockset.lockFor(oldKey);
        } else {
          l1 = lockset.lockFor(oldKey);
          l2 = lockset.lockFor(key);
        }
        l1.lock();
        try {
          l2.lock();
          try {
            Transaction<ByteBuffer, ByteBuffer, ByteBuffer> tx = getTransaction();
            rawRemove(tx, oldKey, oldDataSize);
            rawPut(tx, key, data);
            finishOpenTransaction(tx);
          } finally {
            l2.unlock();
          }
        } finally {
          l1.unlock();
        }
      }
    } catch (TransactionException e) {
      throw ExtinctionType.PERSISTENCE_MUTATE_FAILURE.exception(e);
    }
    storage.markDirty();
  }

  public void finishRestart() {
    List<? extends AbstractRecordContainer<?>> shards = dataContainer.getShards();
    for (int i = 0; i < shards.size(); i++) {
      @SuppressWarnings("unchecked")
      C c = (C) shards.get(i);
      c.finishRestart();
    }
  }

  public ByteBuffer getForLSN(long lsn) throws LSNRetryLookupException {
    return rawGetForLSN(lsn);
  }

  public boolean isDisposed() {
    return dataContainer.isDisposed();
  }

  private ByteBuffer rawGetForLSN(long lsn) {
    try {
      Tuple<ByteBuffer, ByteBuffer, ByteBuffer> ret = getRestartability().get(lsn);
      if (ret != null) {
        ByteBuffer buf = NIOBufferUtils.dup(ret.getValue());
        if (ret instanceof Disposable) {
          ((Disposable) ret).dispose();
        }
        return buf;
      }
    } catch (AssertionError e) {
      if (lsnMap.getAddressForLSN(lsn) == null) {
        //if it is missing, might be a very subtle race. retry.
        throw new LSNRetryLookupException(e, lsn);
      }
      throw e;
    }
    return null;
  }

  protected void notifyOfLSNUpdate(Long key, long priorLSN, long newLSN) {

  }

  protected void notifyOfLSNAssignment(Long key, long lsn, ByteBuffer rValue) {

  }

  public long getAllocatedSupportStorage() {
    return lsnMap.getReservedMemoryStorage();
  }

  public long getOccupiedSupportStorage() {
    return lsnMap.getOccupiedMemoryStorage();
  }

  private final class MapObjectManagerStripe
      implements ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer>, ObjectManagerSegment<ByteBuffer, ByteBuffer, ByteBuffer> {

    @Override
    public Long getLowestLsn() {
      if (isDisposed()) {
        return null;
      }
      AbstractMap.SimpleEntry<Long, Long> ret = lsnMap.firstLSNAndAddress();
      if (ret == null) {
        return null;
      }
      return ret.getKey();
    }

    @Override
    public Long getLsn(ByteBuffer key) {
      if (isDisposed()) {
        return null;
      }
      return lsnMap.getLsn(decodeKey(key));
    }

    @Override
    public void put(ByteBuffer rKey, ByteBuffer rValue, long lsn) {
      if (isDisposed()) {
        return;
      }
      Long key = decodeKey(rKey);
      Lock l = lockset.lockFor(key);
      l.lock();
      try {
        lsnMap.put(key, lsn);
        notifyOfLSNAssignment(key, lsn, rValue);
      } finally {
        l.unlock();
      }
    }

    @Override
    public void remove(ByteBuffer rKey) {
      if (isDisposed()) {
        return;
      }
      Long key = decodeKey(rKey);
      Lock l = lockset.lockFor(key);
      l.lock();
      try {
        lsnMap.remove(key);
      } finally {
        l.unlock();
      }
    }

    @Override
    public void delete() {
      lsnMap.dispose();
    }

    @Override
    public void replayPut(ByteBuffer rKey, ByteBuffer rValue, long lsn) {
      if (isDisposed()) {
        return;
      }

      Long key = decodeKey(rKey);

      Lock l = lockset.lockFor(key);
      l.lock();
      try {
        SovereignRestartableBroker.this.replayPut(lsn, key, rValue);
        byteSize.addAndGet(identifierSize + ENCODED_KEY_SIZE + valueByteSize(rValue));
        lsnMap.put(key, lsn);
      } finally {
        l.unlock();
      }
    }

    @Override
    public Collection<ObjectManagerSegment<ByteBuffer, ByteBuffer, ByteBuffer>> getSegments() {
      return Collections.<ObjectManagerSegment<ByteBuffer, ByteBuffer, ByteBuffer>>singleton(this);
    }

    @Override
    public void updateLsn(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry, long newLsn) {
      if (isDisposed()) {
        return;
      }
      Long key = decodeKey(entry.getKey());
      Lock l = lockset.lockFor(key);
      l.lock();
      try {
        long entryLSN = entry.getLsn();
        Long oldLSN = lsnMap.getLsn(key);
        if (oldLSN != null && entryLSN == oldLSN.longValue()) {
          lsnMap.put(key, newLsn);
          notifyOfLSNUpdate(key, oldLSN, newLsn);
        } else {
          throw new AssertionError();
        }
      } finally {
        l.unlock();
      }
    }

    @SuppressFBWarnings("UL_UNRELEASED_LOCK")
    @Override
    public ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
      // trick here is we need the key to get the right lock, but after
      // getting the key, it might not be the right key anymore. 2 step cha-cha.
      for (; ; ) {
        if (isDisposed() || localDisposed) {
          return null;
        }

        // first get
        AbstractMap.SimpleEntry<Long, Long> ent = lsnMap.firstLSNAndAddress();

        // fail fast
        if (ent == null) {
          // nothing here, no point
          return null;
        }

        // get optimistic lsn/key
        long key = ent.getValue();
        long lsn = ent.getKey();

        // greedy lock
        Lock l = lockset.lockFor(key);
        l.lock();
        try {
          // get lsn/key again
          AbstractMap.SimpleEntry<Long, Long> ent2 = lsnMap.firstLSNAndAddress();

          // if it is a match, good get, go forward.
          if (ent2 != null && ent2.getKey() == lsn && ent2.getValue() == key && !localDisposed) {
            if (lsn >= ceilingLsn) {
              // no point, lsn too high
              l.unlock();
              return null;
            }

            ByteBuffer rKey = encodeKey(key);
            ByteBuffer rValue = rawGetForLSN(lsn);

            return new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(identifier, rKey, rValue, lsn);
          } else {
            // raciness, respin.
            l.unlock();
          }

        } catch (RuntimeException | Error e) {
          l.unlock();
          throw e;
        }
      }
    }

    @Override
    public void releaseCompactionEntry(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry) {
      if (entry == null) {
        throw new NullPointerException("Tried to release a null entry.");
      } else {
        Long key = decodeKey(entry.getKey());
        Lock l = lockset.lockFor(key);
        l.unlock();
      }
    }

    @Override
    public long size() {
      if (isDisposed()) {
        return 0;
      }
      return lsnMap.size();
    }

    @Override
    public long sizeInBytes() {
      if (isDisposed()) {
        return 0;
      }
      return byteSize.get();
    }

    @Override
    public void updateLsn(int hash, ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry, long newLsn) {
      updateLsn(entry, newLsn);
    }

    @Override
    public Long getLsn(int hash, ByteBuffer key) {
      return getLsn(key);
    }

    @Override
    public void put(int hash, ByteBuffer key, ByteBuffer value, long lsn) {
      put(key, value, lsn);
    }

    @Override
    public void replayPut(int hash, ByteBuffer key, ByteBuffer value, long lsn) {
      replayPut(key, value, lsn);
    }

    @Override
    public void remove(int hash, ByteBuffer key) {
      remove(key);
    }
  }
}
