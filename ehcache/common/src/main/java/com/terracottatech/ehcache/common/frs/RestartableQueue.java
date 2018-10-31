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
package com.terracottatech.ehcache.common.frs;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ObjectManagerStripe;
import com.terracottatech.frs.object.RestartableObject;

import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Restartable queue
 *
 * @author tim
 * @author RKAV ported to ehcache 3.0
 */
public class RestartableQueue<T> extends AbstractQueue<T> implements
    RestartableObject<ByteBuffer, ByteBuffer, ByteBuffer> {
  private final ByteBuffer id;
  private final RestartableGenericMap<Long, T> backend;
  private final ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> objectManagerStripe;
  private final SortedSet<Long> modSet = new TreeSet<>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private long modCount = 0;

  public RestartableQueue(ByteBuffer id, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore, Class<T> valueType) {
    this.id = id;
    this.backend = new RestartableGenericMap<>(Long.class, valueType, id, restartStore);
    this.objectManagerStripe = new RestartableQueueObjectManagerStripe(backend.getObjectManagerStripe());
  }

  @Override
  public ByteBuffer getId() {
    return id;
  }

  @Override
  public ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> getObjectManagerStripe() {
    return objectManagerStripe;
  }

  @Override
  public boolean offer(T item) {
    writeLock().lock();
    try {
      long itemModCount = modCount++;
      modSet.add(itemModCount);
      backend.put(itemModCount, item);
      return true;
    } finally {
      writeLock().unlock();
    }
  }

  @Override
  public T peek() {
    readLock().lock();
    try {
      if (modSet.isEmpty()) {
        return null;
      } else {
        return getValue(modSet.first());
      }
    } finally {
      readLock().unlock();
    }
  }

  @Override
  public T poll() {
    writeLock().lock();
    try {
      if (modSet.isEmpty()) {
        return null;
      } else {
        long mod = modSet.first();
        modSet.remove(mod);
        return removeValue(mod);
      }
    } finally {
      writeLock().unlock();
    }
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public Iterator<T> iterator() {
    return new RestartableQueueIterator();
  }

  @Override
  public int size() {
    readLock().lock();
    try {
      return modSet.size();
    } finally {
      readLock().unlock();
    }
  }

  private Iterator<Long> modIterator() {
    readLock().lock();
    try {
      return modSet.iterator();
    } finally {
      readLock().unlock();
    }
  }

  private T getValue(long mod) {
    readLock().lock();
    try {
      return backend.get(mod);
    } finally {
      readLock().unlock();
    }
  }

  private T removeValue(long mod) {
    writeLock().lock();
    try {
      modCount++;
      modSet.remove(mod);
      return backend.remove(mod);
    } finally {
      writeLock().unlock();
    }
  }

  private Lock writeLock() {
    return lock.writeLock();
  }

  private Lock readLock() {
    return lock.readLock();
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      modSet.clear();
      backend.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private class RestartableQueueIterator implements Iterator<T> {
    Iterator<Long> i = modIterator();
    private Long lastModCount = null;

    @Override
    public boolean hasNext() {
      readLock().lock();
      try {
        return i.hasNext();
      } finally {
        readLock().unlock();
      }
    }

    @Override
    public T next() {
      readLock().lock();
      try {
        lastModCount = i.next();
        return backend.get(lastModCount);
      } finally {
        readLock().unlock();
      }
    }

    @Override
    public void remove() {
      writeLock().lock();
      try {
        i.remove(); // Remove from the iterator to keep it from throwing a CME, removeValue() will actually
        // remove the value from the set, but that remove will just wind up being a no-op.
        removeValue(lastModCount);
      } finally {
        writeLock().unlock();
      }
    }
  }

  private class RestartableQueueObjectManagerStripe implements ObjectManagerStripe<ByteBuffer,
      ByteBuffer, ByteBuffer> {
    private final ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> delegate;

    RestartableQueueObjectManagerStripe(ObjectManagerStripe<ByteBuffer, ByteBuffer, ByteBuffer> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Long getLowestLsn() {
      return delegate.getLowestLsn();
    }

    @Override
    public Long getLsn(ByteBuffer key) {
      return delegate.getLsn(key);
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value, long lsn) {
      delegate.put(key, value, lsn);
    }

    @Override
    public void remove(ByteBuffer key) {
      delegate.remove(key);
    }

    @Override
    public void delete() {
      delegate.delete();
    }

    @Override
    public void replayPut(ByteBuffer key, ByteBuffer value, long lsn) {
      // Shouldn't really deadlock in theory, but just grabbing the queue lock out here to
      // make sure we're grabbing locks in the right order.
      writeLock().lock();
      try {
        delegate.replayPut(key, value, lsn);
        long mod = FrsCodecFactory.bufferToLong(key);
        if (mod >= modCount) {
          modCount = mod + 1;
        }
        modSet.add(mod);
      } finally {
        writeLock().unlock();
      }
    }

    @Override
    public Collection<ObjectManagerSegment<ByteBuffer, ByteBuffer, ByteBuffer>> getSegments() {
      return delegate.getSegments();
    }

    @Override
    public void updateLsn(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry, long newLsn) {
      delegate.updateLsn(entry, newLsn);
    }

    @Override
    public void releaseCompactionEntry(ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry) {
      delegate.releaseCompactionEntry(entry);
    }

    @Override
    public long size() {
      return delegate.size();
    }

    @Override
    public long sizeInBytes() {
      return delegate.sizeInBytes();
    }
  }
}
