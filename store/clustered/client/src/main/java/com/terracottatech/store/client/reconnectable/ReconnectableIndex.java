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
package com.terracottatech.store.client.reconnectable;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * An {@link Index} implementation delegating operations to a wrapped {@code Index} instance.
 * This implementation presumes the {@link #on()} and {@link #definition()} methods return static
 * results.
 * @param <T> the value type for the index
 */
final class ReconnectableIndex<T extends Comparable<T>> implements Index<T> {

  private final ReconnectController reconnectController;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  @GuardedBy("lock")
  private volatile Index<T> delegate;

  ReconnectableIndex(Index<T> delegate, ReconnectController reconnectController) {
    this.delegate = delegate;
    this.reconnectController = reconnectController;
  }

  Index<T> getDelegate() {
    Lock lock = this.lock.readLock();
    lock.lock();
    try {
      return delegate;
    } finally {
      lock.unlock();
    }
  }

  void swap(Index<T> newDelegate) {
    Lock lock = this.lock.writeLock();
    lock.lock();
    try {
      delegate = newDelegate;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public CellDefinition<T> on() {
    return getDelegate().on();
  }

  @Override
  public IndexSettings definition() {
    return getDelegate().definition();
  }

  @Override
  public Status status() {
    return reconnectController.withConnectionClosedHandling(() -> getDelegate().status());
  }
}
