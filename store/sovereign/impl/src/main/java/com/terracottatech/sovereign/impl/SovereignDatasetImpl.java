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
package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.DatasetSchema;
import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.exceptions.CellNameCollisionException;
import com.terracottatech.sovereign.exceptions.LockConflictException;
import com.terracottatech.sovereign.exceptions.MalformedCellException;
import com.terracottatech.sovereign.exceptions.RecordLockedException;
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import com.terracottatech.sovereign.impl.dataset.Catalog;
import com.terracottatech.sovereign.impl.dataset.RecordStreamImpl;
import com.terracottatech.sovereign.impl.indexing.SimpleIndex;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexing;
import com.terracottatech.sovereign.impl.memory.AbstractRecordContainer;
import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.MemorySpace;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.memory.ShardIterator;
import com.terracottatech.sovereign.impl.memory.ShardedRecordContainer;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.impl.model.SovereignPrimaryMap;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignSpace;
import com.terracottatech.sovereign.impl.persistence.AbstractPersistentStorage;
import com.terracottatech.sovereign.impl.persistence.AbstractStorage;
import com.terracottatech.sovereign.impl.persistence.SharedSyncher;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.impl.utils.LockSet;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.spi.IndexMapConfig;
import com.terracottatech.sovereign.spi.store.DataContainer;
import com.terracottatech.sovereign.spi.store.PersistentRecord;
import com.terracottatech.sovereign.time.PersistableTimeReferenceGenerator;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.store.Cell;
import com.terracottatech.store.CellCollection;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.internal.InternalRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author cschanck
 */
public class SovereignDatasetImpl<K extends Comparable<K>> implements SovereignDataset<K>, Catalog<K> {

  public static final String SOVEREIGN_KEY_MSN_GUARD_ON_GET_PROP = "sovereign.key_msn_guard_on_get";
  private static final boolean KEY_MSN_GUARD_ON_GET = MiscUtils.getBoolean(SOVEREIGN_KEY_MSN_GUARD_ON_GET_PROP, true);
  private static final Logger LOG = LoggerFactory.getLogger(SovereignDatasetImpl.class);
  private static final int MAX_OPTIMISTIC_READ_TRIES = 10;

  static {
    if (!KEY_MSN_GUARD_ON_GET) {
      LOG.warn("Using slow path key compare key guard. (" + SOVEREIGN_KEY_MSN_GUARD_ON_GET_PROP + ")");
    }
  }

  final Type<K> type;
  // Feathers was here.
  private final AtomicReference<SovereignExtinctionException> extinctionException = new AtomicReference<>(null);
  private final UUID uuid;
  private MemorySpace usage;
  private SovereignPrimaryMap<K> primary;
  private SovereignContainer<K> heap;
  private final SimpleIndexing<K> indexing;
  private final SovereignDataSetConfig<K, ?> config;
  private final LockSet lockset;
  private final long recordLockTimeout;
  private final SovereignRuntime<K> runtime;
  private final Runnable lazySyncherTask;
  private final Runnable immediateSyncherTask;
  private final Object disposeLockObject = new Object();
  private SharedSyncher.SyncRequest persistentSyncRequest;

  /**
   * When {@code true}, this {@code SovereignDataset} may no longer be used.
   */
  private volatile boolean isDisposed = false;

  public SovereignDatasetImpl(SovereignDatasetDescriptionImpl<K, ?> description) {
    this(description.getConfig(), description.getUUID(), description.getConfig().getStorage().newCachingSequence());
  }

  @SuppressWarnings("unchecked")
  public SovereignDatasetImpl(SovereignDataSetConfig<K, ?> config, UUID uuid, CachingSequence sequence) {
    this.uuid = uuid;
    this.config = config.duplicate().freeze();
    this.config.validate();
    try {
      this.runtime = new SovereignRuntime<>(config, sequence);
      this.type = config.getType();
      this.recordLockTimeout = config.getRecordLockTimeout();
      this.lockset = runtime.getLockset();

      usage = (MemorySpace) this.config.getStorage().makeSpace(runtime);

      heap = (SovereignContainer<K>) usage.createContainer();

      primary = (SovereignPrimaryMap<K>) usage.createMap("Primary Key", new IndexMapConfig<K>() {
        @Override
        public boolean isSortedMap() {
          return false;
        }

        @Override
        public Type<K> getType() {
          return config.getType();
        }

        @Override
        public DataContainer<?, ?, ?> getContainer() {
          return heap;
        }
      });

      this.indexing = new SimpleIndexing<>(this);

      config.getStorage().registerNewDataset(this);

      if (config.getStorage().isPersistent()) {
        AbstractPersistentStorage pstore = (AbstractPersistentStorage) config.getStorage();
        this.immediateSyncherTask = () -> {
          pstore.fsynch(false);
        };
        SovereignDatasetDiskDurability dur = config.getDiskDurability();
        switch (dur.getDurabilityEnum()) {
          case NONE:
            this.lazySyncherTask = () -> {};
            break;
          case ALWAYS:
            this.lazySyncherTask = () -> {
              pstore.fsynch(false);
            };
            break;
          case TIMED:
            final SharedSyncher sync = pstore.getSyncher();
            SovereignDatasetDiskDurability.Timed timedDur = (SovereignDatasetDiskDurability.Timed) dur;
            this.persistentSyncRequest = sync.fetchSyncRequest(timedDur.getNsDuration(), TimeUnit.NANOSECONDS);
            this.lazySyncherTask = () -> {
              this.persistentSyncRequest.request();
            };
            break;
          default:
            throw new IllegalStateException();
        }
      } else {
        this.lazySyncherTask = () -> {
        };
        this.immediateSyncherTask = () -> {
        };
      }
    } catch (SovereignExtinctionException e) {
      throw processExtinction(e);
    }
  }

  public void addChangeListener(int shardIndex, ChangeListener listener) {
    getContainer().addChangeListener(shardIndex, listener);
  }

  public void setMutationConsumer(Consumer<BufferDataTuple> mutationConsumer) {
    getContainer().setMutationConsumer(mutationConsumer);
  }

  public void consumeMutationsThenRemoveListener(int shardIndex,
                                                 ChangeListener listener,
                                                 Consumer<Iterable<BufferDataTuple>> mutationConsumer) {
    getContainer().consumeMutationsThenRemoveListener(shardIndex, listener, mutationConsumer);
  }

  public ShardIterator iterate(int shardIndex) {
    AbstractRecordContainer<?> recordContainer = getContainer().getShards().get(shardIndex);
    return new ShardIterator(recordContainer);
  }

  @Override
  public AbstractStorage getStorage() {
    testDisposed();
    return runtime.getConfig().getStorage();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SovereignDatasetImpl{");
    sb.append("isDisposed=").append(isDisposed);
    sb.append(", heap=").append(heap);
    if (usage != null && !usage.isDropped()) {
      sb.append(", used=").append(usage.getUsed());
      sb.append(", capacity=").append(usage.getCapacity());

    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public SovereignRecord<K> add(final Durability durability, final K key, final Iterable<Cell<?>> cells) {
    try {
      return addInternal(this::lock, durability, key, cells);
    } catch (RecordLockedException e) {
      // This should NEVER happen
      throw new AssertionError("Unexpected exception: " + e, e);
    }
  }

  @Override
  public SovereignRecord<K> tryAdd(Durability durability, K key, Iterable<Cell<?>> cells) throws RecordLockedException {
    return addInternal(this::tryLock, durability, key, cells);
  }

  private SovereignRecord<K> addInternal(LockFunction<K> keyLocker,
                                         Durability durability,
                                         K key,
                                         Iterable<Cell<?>> cells) throws RecordLockedException {
    testDisposed();
    validateKey(key);
    CellCollection cellCollection = validateCells(cells);
    Lock lock = keyLocker.apply(key);
    try (ContextImpl c = heap.start(false)) {
      PersistentMemoryLocator old = primary.get(c, key);
      SovereignPersistentRecord<K> record = !old.isEndpoint() ? heap.get(old) : null;
      if (record == null) {
        record = VersionedRecord.single(key,
                                        runtime.getTimeReferenceGenerator().get(),
                                        runtime.getSequence().next(),
                                        cellCollection);
        PersistentMemoryLocator loc = heap.add(record);
        LOG.trace("Record {} added to {}", record, loc);
        primary.put(c, key, loc);
        record.setLocation(loc);
        indexing.recordIndex(c, null, null, record, loc);
        postMutation(durability);
        record = null;
      }
      return record;
    } catch (SovereignExtinctionException e) {
      throw processExtinction(e);
    } finally {
      lock.unlock();
    }
  }

  long getGuardMSN() {
    long ret = runtime.getSequence().current();
    long msnsPossiblyInFlight = lockset.getSize();
    if ((Long.MIN_VALUE + msnsPossiblyInFlight) >= ret) {
      return Long.MIN_VALUE;
    }
    return ret - msnsPossiblyInFlight;
  }

  boolean wasStableRecordRead(long guardMSN, K key, SovereignRecord<K> record) {
    if (KEY_MSN_GUARD_ON_GET) {
      long rmsn = record.getMSN();
      boolean guarded = rmsn < guardMSN;
      if (guarded) {
        return true;
      }
    }
    return record.getKey().equals(key);
  }

  @Override
  public SovereignPersistentRecord<K> get(final K key) {
    try {
      return getInternal(this::lock, key);
    } catch (RecordLockedException e) {
      // This should NEVER happen
      throw new AssertionError("Unexpected exception: " + e, e);
    }
  }

  @Override
  public SovereignRecord<K> tryGet(K key) throws RecordLockedException {
    return getInternal(this::tryLock, key);
  }

  private SovereignPersistentRecord<K> getInternal(LockFunction<K> keyLocker, K key) throws RecordLockedException {
    testDisposed();
    validateKey(key);
    try (ContextImpl c = heap.start(false)) {
      for (int tries = 0; tries < MAX_OPTIMISTIC_READ_TRIES; tries++) {
        long guardMSN = getGuardMSN();
        SovereignPersistentRecord<K> record = rawGet(c, key);
        if (record == null) {
          return null;
        }
        if (wasStableRecordRead(guardMSN, key, record)) {
          return record;
        }
      }
      // lock and try again
      Lock lock = keyLocker.apply(key);
      try {
        return rawGet(c, key);
      } finally {
        lock.unlock();
      }
    } catch (SovereignExtinctionException e) {
      throw processExtinction(e);
    }
  }

  private SovereignPersistentRecord<K> rawGet(ContextImpl c, K key) {
    PersistentMemoryLocator loc = primary.get(c, key);
    if (loc.isEndpoint()) {
      return null;
    }
    SovereignPersistentRecord<K> record = heap.get(loc);
    if (record == null) {
      return null;
    }
    record.setLocation(loc);
    return record;
  }

  @Override
  public Consumer<Record<K>> delete(final Durability durability) {
    return new LockingAction<>((ContextImpl cxt, Record<K> record) -> {
      // Dataset disposal checked in LockingAction.apply
      deleteInternal(durability, cxt, record);
      return record;
    });
  }

  @Override
  public <T> Function<Record<K>, T> delete(Durability durability, Function<? super Record<K>, T> output) {
    return new LockingAction<>((ContextImpl cxt, Record<K> record) -> {
      // Dataset disposal checked in LockingAction.apply
      deleteInternal(durability, cxt, record);
      return output.apply(record);
    });
  }

  private void deleteInternal(Durability durability, ContextImpl cxt, Record<K> record) {
    SovereignPersistentRecord<?> pr = ((SovereignPersistentRecord<?>) record);
    PersistentMemoryLocator target = pr.getLocation();
    if (primary.remove(cxt, record.getKey(), target)) {
      indexing.recordIndex(cxt, record, target, null, null);
      heap.delete(target);
      postMutation(durability);
    }
    pr.setLocation(null);
  }

  private void postMutation(Durability durability) {
    switch (durability) {
      case IMMEDIATE:
        immediateSyncherTask.run();
        break;
      case LAZY:
        lazySyncherTask.run();
        break;
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public ShardedRecordContainer<K, ?> getContainer() {
    testDisposed();
    return (ShardedRecordContainer<K, ?>) heap;
  }

  @Override
  public long getCurrentMSN() {
    return runtime.getSequence().current();
  }

  @Override
  public SimpleIndexing<K> getIndexing() {
    return indexing;
  }

  /**
   * If secondary (non-primary) indexing is enabled, returns the
   * {@link com.terracottatech.sovereign.indexing.SovereignIndexSettings#btree() btree} index defined
   * over the {@code CellDefinition} provided.
   *
   * @param def the {@code CellDefinition} for which the index is sought
   * @param <T> the value type for the {@code CellDefinition} and index
   * @return the {@code SortedIndexMap} corresponding to {@code def}
   * @throws java.lang.AssertionError if secondary indexing is disabled
   */
  @Override
  public <T extends Comparable<T>> SovereignSortedIndexMap<T, K> getSortedIndexFor(CellDefinition<T> def) {
    testDisposed();
    if (hasSortedIndex(def)) {
      @SuppressWarnings("unchecked")
      final SimpleIndex<T, K> simpleIndex = (SimpleIndex<T, K>) indexing.getIndex(def, SovereignIndexSettings.btree());
      if (simpleIndex.isLive()) {
        return (SovereignSortedIndexMap<T, K>) simpleIndex.getUnderlyingIndex();
      }
    }
    throw new AssertionError("no index found");
  }

  /**
   * Indicates whether or not a secondary index exists for the {@code CellDefinition} specified.
   *
   * @param def the {@code CellDefinition} for which the index is sought
   * @param <T> the value type for the {@code CellDefinition} and index
   * @return {@code true} if secondary indexing is enabled and a
   * {@link com.terracottatech.sovereign.indexing.SovereignIndexSettings#btree() btree} index
   * exists for {@code def}; {@code false} otherwise
   */
  @Override
  public <T extends Comparable<T>> boolean hasSortedIndex(CellDefinition<T> def) {
    testDisposed();
    if (!Catalog.DISABLE_SECONDARY_INDEXES) {
      SovereignIndex<T> ret = indexing.getIndex(def, SovereignIndexSettings.btree());
      return ret != null && ret.isLive();
    }
    return false;
  }

  @Override
  public <T> Optional<T> applyMutation(final Durability durability,
                                       final K key,
                                       Predicate<? super Record<K>> predicate,
                                       final Function<? super Record<K>, Iterable<Cell<?>>> transform,
                                       final BiFunction<? super Record<K>, ? super Record<K>, T> output) {
    try {
      return applyMutationInternal(this::lock, durability, key, predicate, transform, output);
    } catch (RecordLockedException e) {
      // This should NEVER happen
      throw new AssertionError("Unexpected exception: " + e, e);
    }
  }

  @Override
  public <T> Optional<T> tryApplyMutation(Durability durability,
                                          K key,
                                          Predicate<? super Record<K>> predicate,
                                          Function<? super Record<K>, Iterable<Cell<?>>> transform,
                                          BiFunction<? super Record<K>, ? super Record<K>, T> output) throws RecordLockedException {
    return applyMutationInternal(this::tryLock, durability, key, predicate, transform, output);
  }

  private <T> Optional<T> applyMutationInternal(LockFunction<K> keyLocker,
                                                Durability durability,
                                                K key,
                                                Predicate<? super Record<K>> predicate,
                                                Function<? super Record<K>, Iterable<Cell<?>>> transform,
                                                BiFunction<? super Record<K>, ? super Record<K>, T> output) throws RecordLockedException {
    testDisposed();
    validateKey(key);
    Objects.requireNonNull(predicate, "Predicate must not be null");
    Objects.requireNonNull(transform, "Transform must not be null");
    Objects.requireNonNull(output, "Output function must not be null");
    Lock lock = keyLocker.apply(key);
    try {
      final SovereignPersistentRecord<K> record = get(key);
      if (record == null) {
        return Optional.empty();
      }
      if (!predicate.test(record)) {
        return Optional.empty();
      }
      final Iterable<Cell<?>> newCells = transform.apply(record);
      try (ContextImpl c = heap.start(false)) {
        Optional<T> ret = Optional.ofNullable(output.apply(record, recordMutation(durability, c, record, newCells)));
        postMutation(durability);
        return ret;
      }
    } catch (SovereignExtinctionException e) {
      throw processExtinction(e);
    } finally {
      lock.unlock();
    }
  }

  private Record<K> recordMutation(Durability durability,
                                   ContextImpl cxt,
                                   SovereignPersistentRecord<K> old,
                                   Iterable<Cell<?>> cells) {
    PersistentMemoryLocator loc = old.getLocation();
    if (cells != old) {
      // Suppress validation if we're doing a record 'touch'
      validateCells(cells);
    }

    VersionedRecord<K> newRecord = new VersionedRecord<>(old,
                                                         cells,
                                                         runtime.getTimeReferenceGenerator().get(),
                                                         runtime.getSequence().next(),
                                                         runtime.getRecordConstructionFilter());

    PersistentMemoryLocator newloc = heap.replace(loc, newRecord);

    newRecord.setLocation(newloc);
    primary.put(cxt, old.getKey(), newloc);

    indexing.recordIndex(cxt, old, loc, newRecord, newloc);
    postMutation(durability);

    return newRecord;
  }

  private void validateKey(final K key) {
    Objects.requireNonNull(key, "Record key must not be null");
    if (!this.type.getJDKType().isInstance(key)) {
      throw new ClassCastException("Expecting record key type " + this.type.getJDKType()
        .getName() + ": found " + key.getClass().getName());
    }
  }

  private CellCollection validateCells(Iterable<Cell<?>> cells) {
    Objects.requireNonNull(cells);
    CellSet s = new CellSet();
    for (Cell<?> cell : cells) {
      if (cell == null) {
        throw new NullPointerException("Cells collection contains null entries");
      }
      final CellDefinition<?> cellDefinition = cell.definition();
      if (cellDefinition == null) {
        throw new MalformedCellException("Cell contains no CellDefinition");
      }
      if (cell.value() == null) {
        throw new MalformedCellException("Cell[" + cellDefinition.name() + "] contains a null value");
      }
      if (!cellDefinition.type().getJDKType().isInstance(cell.value())) {
        throw new MalformedCellException("Cell[" + cellDefinition.name() + "] expecting value type " + cellDefinition.type()
          .getJDKType()
          .getName() + ": found " + cell.value().getClass().getName());
      }
      Cell<?> probe = s.set(cell);
      if (probe != null) {
        throw new CellNameCollisionException(cellDefinition, probe.definition());
      }
    }
    return s;
  }

  @Override
  public <T> Function<Record<K>, T> applyMutation(final Durability durability,
                                                  final Function<? super Record<K>, Iterable<Cell<?>>> transform,
                                                  final BiFunction<? super Record<K>, ? super Record<K>, T> output) {
    testDisposed();
    return new LockingAction<>((ContextImpl cxt, Record<K> record) -> {
      // Dataset disposal checked in LockingAction.apply
      Record<K> nr = recordMutation(durability, cxt, (SovereignPersistentRecord<K>) record, transform.apply(record));
      return output.apply(record, nr);
    });
  }

  @Override
  public void applyMutation(final Durability durability,
                            final K key,
                            Predicate<? super Record<K>> predicate,
                            final Function<? super Record<K>, Iterable<Cell<?>>> transform) {
    applyMutation(durability, key, predicate, transform, (a, b) -> Optional.empty());
  }

  @Override
  public void tryApplyMutation(Durability durability,
                               K key,
                               Predicate<? super Record<K>> predicate,
                               Function<? super Record<K>, Iterable<Cell<?>>> transform) throws RecordLockedException {
    tryApplyMutation(durability, key, predicate, transform, (a, b) -> Optional.empty());
  }

  @Override
  public Consumer<Record<K>> applyMutation(final Durability durability,
                                           final Function<? super Record<K>, Iterable<Cell<?>>> transform) {
    return new LockingAction<>((ContextImpl cxt, Record<K> record) -> {
      // Dataset disposal checked in LockingAction.apply
      return recordMutation(durability, cxt, (SovereignPersistentRecord<K>) record, transform.apply(record));
    });
  }

  @Override
  public SovereignRecord<K> delete(final Durability durability, final K key) {
    try {
      return deleteInternal(this::lock, durability, key);
    } catch (RecordLockedException e) {
      // This should NEVER happen
      throw new AssertionError("Unexpected exception: " + e, e);
    }
  }

  @Override
  public SovereignRecord<K> tryDelete(Durability durability, K key) throws RecordLockedException {
    return deleteInternal(this::tryLock, durability, key);
  }

  private SovereignRecord<K> deleteInternal(LockFunction<K> keyLocker,
                                            Durability durability,
                                            K key) throws RecordLockedException {
    testDisposed();
    validateKey(key);
    Lock lock = keyLocker.apply(key);
    try (ContextImpl c = heap.start(false)) {
      PersistentMemoryLocator remove = primary.get(c, key);
      if (remove.isEndpoint()) {
        return null;
      }
      SovereignPersistentRecord<K> record = heap.get(remove);
      heap.delete(remove);
      primary.remove(c, key, remove);
      if (record != null) {
        indexing.recordIndex(c, record, remove, null, null);
        record.setLocation(null);
      }
      return record;
    } catch (SovereignExtinctionException e) {
      throw processExtinction(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public SovereignRecord<K> delete(final Durability durability,
                                   final K key,
                                   final Predicate<? super Record<K>> predicate) {
    try {
      return deleteInternal(this::lock, durability, key, predicate);
    } catch (RecordLockedException e) {
      // This should NEVER happen
      throw new AssertionError("Unexpected exception: " + e, e);
    }
  }

  @Override
  public SovereignRecord<K> tryDelete(Durability durability,
                                      K key,
                                      Predicate<? super Record<K>> predicate) throws RecordLockedException {
    return deleteInternal(this::tryLock, durability, key, predicate);
  }

  private SovereignRecord<K> deleteInternal(LockFunction<K> keyLocker,
                                            Durability durability,
                                            K key,
                                            Predicate<? super Record<K>> predicate) throws RecordLockedException {
    testDisposed();
    validateKey(key);
    Objects.requireNonNull(predicate, "Predicate must not be null");
    Lock lock = keyLocker.apply(key);
    try (ContextImpl c = heap.start(false)) {
      PersistentMemoryLocator loc = primary.get(c, key);
      if (!loc.isEndpoint()) {
        SovereignPersistentRecord<K> value = heap.get(loc);
        if (value != null && predicate.test(value)) {
          heap.delete(loc);
          primary.remove(c, key, loc);
          indexing.recordIndex(c, value, loc, null, null);
          value.setLocation(null);
          return value;
        }
      }
    } catch (SovereignExtinctionException e) {
      throw processExtinction(e);
    } finally {
      lock.unlock();
    }
    return null;
  }

  @Override
  public RecordStream<K> records() {
    testDisposed();
    return RecordStreamImpl.newInstance(this, false);
  }

  @Override
  public void flush() {
    try {
      postMutation(Durability.IMMEDIATE);
    } catch (SovereignExtinctionException e) {
      throw processExtinction(e);
    }
  }

  /**
   * This method is to be called only by the backend storage object.
   * This implementation marks this {@code Dataset} as disposed and disposes of the
   * subordinate objects held by this {@code Dataset}.  This method is <b>not</b>
   * thread-safe; the caller must ensure no other operations with this {@code Dataset}
   * are in progress when this method is called.  Operations making use of {@code Dataset}
   * resources following a call to this method are likely to be aborted with an
   * {@link java.lang.IllegalStateException}.<p>
   * This method is designed to be called only from the storage backend for this dataset.
   */
  public void dispose() {
    if (this.isDisposed) {
      return;
    }
    synchronized (disposeLockObject) {
      if (this.isDisposed) {
        return;
      }
      this.isDisposed = true;

      // stop syncing caring about me
      if (persistentSyncRequest != null) {
        persistentSyncRequest.release();
      }

      // Heap is disposed first to induce interruption of any in-progress indexing
      if (heap != null) {
        this.heap.dispose();
        this.heap = null;
      }

      if (this.indexing != null) {
        this.indexing.getIndexes().forEach(s -> {
          try {
            this.indexing.destroyIndex(s, false);
          } catch (Exception e) {
            // Ignored
          }
        });
      }
      if (primary != null) {
        this.primary.drop();
        this.primary = null;
      }

      if (usage != null) {
        this.usage.drop();
        this.usage = null;
      }

      if (runtime != null) {
        this.runtime.getSchema().getBackend().dispose();

        // detach all callbacks, because they kep refs: sequence, timeref,
        this.runtime.getSequence().setCallback((s) -> {});
        // TODO this just sucks, but this persistent/non split kinda sucks too. Perhaps we can fix that at some
        // point.
        @SuppressWarnings("rawtypes")
        final TimeReferenceGenerator<? extends TimeReference> timeReferenceGenerator = getTimeReferenceGenerator();
        if (timeReferenceGenerator instanceof PersistableTimeReferenceGenerator) {
          PersistableTimeReferenceGenerator<?> pgen = (PersistableTimeReferenceGenerator) timeReferenceGenerator;
          pgen.setPersistenceCallback(() -> null);
        }
        runtime.dispose();
      }
    }
  }

  @Override
  public boolean isDisposed() {
    return this.isDisposed;
  }

  private void testDisposed() throws IllegalStateException {
    if (this.isDisposed) {
      throw new IllegalStateException("Attempt to use disposed dataset");
    }
  }

  /**
   * Obtain the lock for {@code key} and attempt to acquire the lock within
   * {@link #recordLockTimeout} milliseconds.
   *
   * @param key the key instance for which the lock is obtained
   * @return the {@code Lock} instance if the lock is acquired
   * @throws LockConflictException if the lock was not acquired within {@link #recordLockTimeout} milliseconds
   */
  private Lock lock(K key) {
    final ReentrantLock lock = lockset.lockFor(key);
    boolean isInterrupted = false;
    try {
      long startTime = 0;
      long waitRemaining = 0;     // Quick try before overhead of System.nanoTime and math
      do {
        try {
          if (lock.tryLock(waitRemaining, TimeUnit.NANOSECONDS)) {
            return lock;
          }
        } catch (InterruptedException e) {
          /*
           * ReentrantLock.tryLock will throw an InterruptedException if entered while the thread
           * is interrupted. Remember but do not otherwise react to the interruption; wait for full
           * recordLockTimeout.
           */
          isInterrupted = true;
        }
        if (startTime == 0) {
          startTime = System.nanoTime();
        }
        waitRemaining = startTime + TimeUnit.MILLISECONDS.toNanos(this.recordLockTimeout) - System.nanoTime();
      } while (waitRemaining > 0);
    } finally {
      if (isInterrupted) {
        Thread.currentThread().interrupt();
      }
    }

    throw new LockConflictException(String.format(
      "Failed to obtain lock on key='%s' within %d ms; ensure SovereignDataset.applyMutation lambdas contain no locking",
      key,
      this.recordLockTimeout));
  }

  /**
   * Attempts to obtain the lock for {@code key}.  If the lock is not immediately available, an exception is
   * thrown.
   *
   * @param key the key instance for which the lock is obtained
   * @return the {@code Lock} instance if the lock is acquired
   * @throws RecordLockedException if the lock for {@code key} is not immediately available
   */
  private Lock tryLock(K key) throws RecordLockedException {
    ReentrantLock lock = lockset.lockFor(key);
    if (lock.tryLock()) {
      return lock;
    } else {
      throw new RecordLockedException(String.format("Lock for key='%s' not immediately available", key));
    }
  }

  public SovereignSpace getUsage() {
    testDisposed();
    return usage;
  }

  public SovereignPrimaryMap<K> getPrimary() {
    testDisposed();
    return primary;
  }

  @Override
  public Type<K> getType() {
    return type;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public TimeReferenceGenerator<? extends TimeReference> getTimeReferenceGenerator() {
    return runtime.getTimeReferenceGenerator();
  }

  public CachingSequence getSequence() {
    return runtime.getSequence();
  }

  public SovereignDataSetConfig<K, ?> getConfig() {
    return config;
  }

  @Override
  public UUID getUUID() {
    return uuid;
  }

  @Override
  public SovereignDatasetDescriptionImpl<K, ?> getDescription() {
    return new SovereignDatasetDescriptionImpl<>(this);
  }

  @Override
  public String getAlias() {
    return getDescription().getAlias();
  }

  public SovereignRuntime<K> getRuntime() {
    return runtime;
  }

  @Override
  public DatasetSchema getSchema() {
    return runtime.getSchema();
  }

  private SovereignExtinctionException processExtinction(SovereignExtinctionException e) {
    LOG.error("Dataset Extinction", e);
    extinctionException.compareAndSet(null, e);
    // this is an odd case -- dispose the dataset but leave it in the storage
    // needed for understanding the situation
    dispose();
    // always return the first exception.
    return extinctionException.get();
  }

  @Override
  public SovereignExtinctionException getExtinctionException() {
    return extinctionException.get();
  }

  @Override
  public long getAllocatedHeapStorageSize() {
    try {
      return heap.getReserved();
    } catch (Exception e) {
      LOG.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long getOccupiedHeapStorageSize() {
    try {
      return heap.getUsed();
    } catch (Exception e) {
      LOG.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long getAllocatedPrimaryKeyStorageSize() {
    try {
      return primary.getAllocatedStorageSize();
    } catch (Exception e) {
      LOG.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  public long getAllocatedIndexStorageSize() {
    long total = 0;
    try {
      for (SovereignIndex<?> si : getIndexing().getIndexes()) {
        try {
          total = total + ((SimpleIndex) si).getUnderlyingIndex().getAllocatedStorageSize();
        } catch (Exception ee) {
          LOG.debug("Oddity fetching statistic", ee);
        }
      }
    } catch (Exception e) {
      LOG.debug("Oddity fetching statistic", e);
    }
    return total;
  }

  @Override
  public long getOccupiedPrimaryKeyStorageSize() {
    try {
      return primary.getOccupiedStorageSize();
    } catch (Exception e) {
      LOG.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long getPersistentBytesUsed() {
    try {
      return heap.getPersistentBytesUsed();
    } catch (Exception e) {
      LOG.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long getAllocatedPersistentSupportStorage() {
    return heap.getAllocatedPersistentSupportStorage();
  }

  @Override
  public long getOccupiedPersistentSupportStorage() {
    return heap.getOccupiedPersistentSupportStorage();
  }

  @Override
  public long recordCount() {
    try {
      return heap.count();
    } catch (Exception e) {
      LOG.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @FunctionalInterface
  private interface LockFunction<T> {
    Lock apply(T t) throws RecordLockedException;
  }

  private class LockingAction<T>
    implements ManagedAction<K>, Consumer<Record<K>>, Function<Record<K>, T>, AutoCloseable {

    /**
     * A {@code ThreadLocal} holding the context required for operation of this {@code LockingAction} in
     * a given {@code Stream} thread.  A single {@code LockingAction} instance (created through
     * {@link SovereignDataset#applyMutation(Durability, Function) applyMutation(Durability, Function)},
     * {@link SovereignDataset#applyMutation(Durability, Function, BiFunction)
     * applyMutation(Durability, Function, BiFunction},
     * or {@link SovereignDatasetImpl#delete(Durability) delete(Durability)}) can be instantiated and <i>shared</i>
     * among several stream pipelines <b>and</b>, potentially, among several threads running a
     * single <i>parallel</i> pipeline.
     */
    private final ThreadLocal<LockingActionContext> streamContext = ThreadLocal.withInitial(LockingActionContext::new);

    private final BiFunction<ContextImpl, Record<K>, T> action;

    private LockingAction(BiFunction<ContextImpl, Record<K>, T> lockedAction) {
      this.action = lockedAction;
    }

    @Override
    public InternalRecord<K> begin(InternalRecord<K> record) {
      testDisposed();
      final LockingActionContext context = this.streamContext.get();
      context.lock = lock(record.getKey());
      context.cxt = heap.start(true);
      PersistentMemoryLocator loc = primary.get(context.cxt, record.getKey());
      if (!loc.isEndpoint() && record instanceof PersistentRecord) {
        context.current = heap.get(loc);      // TODO: Implement something more performant (TDB-2035)
      } else {
        context.current = get(record.getKey());
      }
      return context.current;
    }

    @Override
    public void end(InternalRecord<K> record) {
      final LockingActionContext context = this.streamContext.get();
      if (record != context.current) {
        throw new IllegalStateException("record processing not properly ended");
      }
      context.lock.unlock();
      context.lock = null;
      context.cxt.close();
      context.cxt = null;
    }

    @Override
    public T apply(Record<K> record) {
      testDisposed();
      try {
        final LockingActionContext context = this.streamContext.get();
        if (context.lock == null) {
          throw new IllegalStateException(
            "Consumer produced by applyMutation or delete must be passed directly to a records stream produced by the same dataset");
        }
        return action.apply(context.cxt, record);
      } catch (SovereignExtinctionException e) {
        throw processExtinction(e);
      }
    }

    @Override
    public void accept(Record<K> t) {
      apply(t);
    }

    @Override
    public SovereignContainer<K> getContainer() {
      return heap;
    }

    @Override
    public void close() {
      final LockingActionContext context = this.streamContext.get();
      if (context.lock != null) {
        try {
          this.end(context.current);
        } catch (IllegalStateException e) {
          // Ignored
        }
      }
      this.streamContext.remove();
    }

    private final class LockingActionContext {
      private Lock lock;
      private ContextImpl cxt;
      private InternalRecord<K> current;
    }
  }
}
