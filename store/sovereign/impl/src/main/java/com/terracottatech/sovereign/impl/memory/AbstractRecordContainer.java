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
import com.terracottatech.sovereign.common.utils.SimpleFinalizer;
import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.spi.store.LocatorFactory;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author cschanck
 **/
public abstract class AbstractRecordContainer<K extends Comparable<K>>
  implements SovereignContainer<K> {

  private static Logger LOG = LoggerFactory.getLogger(AbstractRecordContainer.class);
  protected final TimeReferenceGenerator<? extends TimeReference<?>> timeReferenceGenerator;
  protected final BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> versionLimitFunction;
  protected final RecordBufferStrategy<K> bufferStrategy;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
  protected final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();
  protected final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
  private final SovereignRuntime<K> runtime;
  private final int shardIndex;
  private volatile boolean disposed = false;

  private volatile Consumer<BufferDataTuple> mutationConsumer;
  private final RecordContainerChangeListener changeListener = new RecordContainerChangeListener();

  public AbstractRecordContainer(ShardSpec shardSpec, SovereignRuntime<K> runtime) {
    this.shardIndex = shardSpec.getShardIndex();
    this.runtime = runtime;
    this.timeReferenceGenerator = runtime.getTimeReferenceGenerator();
    this.versionLimitFunction = runtime.getRecordRetrievalFilter();
    this.bufferStrategy = runtime.getBufferStrategy();
  }

  public void setMutationConsumer(Consumer<BufferDataTuple> mutationConsumer) {
    this.mutationConsumer = mutationConsumer;
  }

  @Override
  public long getAllocatedPersistentSupportStorage() {
    return 0l;
  }

  @Override
  public long getOccupiedPersistentSupportStorage() {
    return 0l;
  }

  @Override
  public long getPersistentBytesUsed() {
    return 0l;
  }

  public void addChangeListener(ChangeListener listener) {
    writeLock.lock();
    try {
      LOG.info("Start listening for Change for shard {}", shardIndex);
      this.changeListener.addChangeListener(listener);
    } finally {
      writeLock.unlock();
    }
  }

  private void removeChangeListener(ChangeListener listener) {
    writeLock.lock();
    try {
      LOG.info("Stop listening for Change for shard {}", shardIndex);
      this.changeListener.removeChangeListener(listener);
    } finally {
      writeLock.unlock();
    }
  }

  public void consumeMutationsThenRemoveListener(ChangeListener listener, Consumer<Iterable<BufferDataTuple>> mutationConsumer) {
    writeLock.lock();
    try {
      mutationConsumer.accept(listener.getChanges());
      removeChangeListener(listener);
    } finally {
      writeLock.unlock();
    }
  }

  private void listenToChange(PersistentMemoryLocator locator, ByteBuffer data) {
    this.changeListener.listen(locator, data);
  }

  private void consumeMutation(PersistentMemoryLocator locator, ByteBuffer data) {
    Consumer<BufferDataTuple> current = mutationConsumer;
    if (current != null) {
      current.accept(new BufferDataTuple() {
        @Override
        public long index() {
          return locator.index();
        }

        @Override
        public ByteBuffer getData() {
          if (data != null) {
            return data.duplicate();
          } else {
            return null;
          }
        }
      });
    }
  }

  public SimpleFinalizer<ContextImpl, ContextImpl.State> getContextFinalizer() {
    return runtime.getContextFinalizer();
  }

  @Override
  public PersistentMemoryLocator add(SovereignPersistentRecord<K> data) {
    testDisposed();
    ByteBuffer buf = bufferStrategy.toByteBuffer(data);
    PersistentMemoryLocator locator = null;
    writeLock.lock();
    try {
      locator = getBufferContainer().add(buf);
      listenToChange(locator, buf);
    } finally {
      writeLock.unlock();
    }
    //This is safe outside the lock since all the mutations Stream or CUD are locked using Locking action lock
    if (locator != null) {
      consumeMutation(locator, buf);
    }
    return locator;
  }

  @Override
  public PersistentMemoryLocator reinstall(long lsn, long persistentKey, ByteBuffer data) {
    this.testDisposed();
    writeLock.lock();
    try {
      PersistentMemoryLocator loc = getBufferContainer().reinstall(lsn, persistentKey, data);
      return loc;
    } finally {
      writeLock.unlock();
    }
  }

  public K deleteIfPresent(long index) {
    testDisposed();
    writeLock.lock();
    try {
      K key = null;
      ByteBuffer buffer = getBufferContainer().getForSlot(index);
      if (buffer != null) {
        getBufferContainer().deleteIfPresent(index);
        key = bufferStrategy.readKey(buffer);
      }
      return key;
    } finally {
      writeLock.unlock();
    }
  }

  public K restore(long persistentKey, ByteBuffer data) {
    testDisposed();
    writeLock.lock();
    try {
      K key = bufferStrategy.readKey(data);
      ByteBuffer oldData = getBufferContainer().getForSlot(persistentKey);
      getBufferContainer().restore(persistentKey, data, oldData);
      return key;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean delete(PersistentMemoryLocator key) {
    testDisposed();
    writeLock.lock();
    boolean result = false;
    try {
      listenToChange(key, null);
      result = getBufferContainer().delete(key);
    } finally {
      writeLock.unlock();
    }
    consumeMutation(key, null);
    return result;
  }

  @Override
  public PersistentMemoryLocator replace(PersistentMemoryLocator key, SovereignPersistentRecord<K> data) {
    testDisposed();
    ByteBuffer buf = bufferStrategy.toByteBuffer(data);
    PersistentMemoryLocator locator = null;
    writeLock.lock();
    try {
      locator = getBufferContainer().replace(key, buf);
      listenToChange(locator, buf);
    } finally {
      writeLock.unlock();
    }
    if(locator != null) {
      consumeMutation(locator, buf);
    }
    return locator;
  }

  @Override
  public SovereignPersistentRecord<K> get(PersistentMemoryLocator key) {
    readLock.lock();
    try {
      testDisposed();
      ByteBuffer buf = null;
      SovereignPersistentRecord<K> rec;
      buf = getBufferContainer().get(key);
      if (buf == null) {
        return null;
      }
      rec = bufferStrategy.fromByteBuffer(buf);
      rec.setLocation(key);
      return prune(rec);
    } finally {
      readLock.unlock();
    }
  }

  public ByteBuffer get(long index) {
    readLock.lock();
    try {
      testDisposed();
      return getBufferContainer().get(new PersistentMemoryLocator(index, null));
    } finally {
      readLock.unlock();
    }
  }

  protected SovereignPersistentRecord<K> prune(SovereignPersistentRecord<K> record) {
    record.prune(timeReferenceGenerator.get(), versionLimitFunction);
    return record;
  }

  @Override
  public PersistentMemoryLocator first(ContextImpl context) {
    this.testDisposed();
    return getBufferContainer().first(context);
  }

  @Override
  public PersistentMemoryLocator last() {
    this.testDisposed();
    return PersistentMemoryLocator.INVALID;
  }

  @Override
  public void drop() {
    writeLock.lock();
    try {
      this.testDisposed();
      throw new UnsupportedOperationException();
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void dispose() {
    writeLock.lock();
    try {
      if (this.disposed) {
        return;
      }
      this.disposed = true;
      this.getBufferContainer().dispose();
      listenToChange(null, null);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean isDisposed() {
    return disposed;
  }

  @Override
  public ContextImpl start(boolean guard) {
    this.testDisposed();
    ContextImpl ctx = new ContextImpl(this, guard);
    return ctx;
  }

  @Override
  public void end(ContextImpl loc) {
    loc.close();
  }

  @Override
  public long count() {
    return getBufferContainer().count();
  }

  @Override
  public PersistentMemoryLocator createLocator(long slot, LocatorFactory factory) {
    this.testDisposed();
    return new PersistentMemoryLocator(slot, factory);
  }

  @Override
  public long mapLocator(PersistentMemoryLocator ref) {
    this.testDisposed();
    return ref.index();
  }

  protected final void testDisposed() throws IllegalStateException {
    if (this.disposed) {
      throw new IllegalStateException("Attempt to use disposed data container");
    }
  }

  @Override
  public long getUsed() {
    return getBufferContainer().getUsed();
  }

  @Override
  public long getReserved() {
    return getBufferContainer().getReserved();
  }

  public abstract MemoryBufferContainer getBufferContainer();

  @Override
  public SovereignRuntime<K> runtime() {
    return runtime;
  }

  public int getShardIndex() {
    return shardIndex;
  }

}
