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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.frs.NotPausedException;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.Statistics;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.recovery.RecoveryException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Wrapper around a RestartStore that allows Transactions to be created in an externally
 * controlled manner. If no active transaction exists for the thread, fallback to an
 * autocommit transaction. The implication here is that restartable objects directly using
 * the {@link RestartStore} interface will not able to begin transactions. This is currently
 * fine as there is no need for multi-part transactions at least anywhere in ehcache.
 *
 * @author tim
 */
public class ControlledTransactionRestartStore<I, K, V> implements RestartStore<I, K, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControlledTransactionRestartStore.class);

  private final ConcurrentMap<I, WrappedTransaction> globalTransactionList =
      new ConcurrentHashMap<>();
  private final ThreadLocal<WrappedTransaction> activeTransaction = new ThreadLocal<>();
  private final RestartStore<I, K, V> delegate;

  public ControlledTransactionRestartStore(RestartStore<I, K, V> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Future<Void> startup() throws InterruptedException, RecoveryException {
    return delegate.startup();
  }

  @Override
  public void shutdown() throws InterruptedException {
    delegate.shutdown();
  }

  @Override
  public Transaction<I, K, V> beginTransaction(boolean synchronous) {
    WrappedTransaction activeTxn = activeTransaction.get();
    if (activeTxn != null) {
      if (activeTxn.synchronous != synchronous) {
        throw new IllegalArgumentException(
            "Mismatched synchronous attributes is unsupported.");
      }
      return activeTxn;
    } else {
      return beginAutoCommitTransaction(synchronous);
    }
  }

  @Override
  public Transaction<I, K, V> beginAutoCommitTransaction(boolean synchronous) {
    return delegate.beginAutoCommitTransaction(synchronous);
  }

  @Override
  public Tuple<I, K, V> get(long l) {
    return delegate.get(l);
  }

  public Transaction<I, K, V> beginEntityTransaction(boolean synchronous, I entityId) {
    WrappedTransaction globalTransaction = getGlobalTransaction(entityId);
    if (globalTransaction != null) {
      return globalTransaction;
    }
    return beginTransaction(synchronous);
  }

  public void beginControlledTransaction(boolean synchronous, I entityId) {
    WrappedTransaction globalTransaction = getGlobalTransaction(entityId);
    if (globalTransaction == null) {
      if (activeTransaction.get() != null) {
        throw new UnsupportedOperationException("Nested controlled transactions are not supported.");
      }
      activeTransaction.set(new WrappedTransaction(synchronous));
    } else {
      activeTransaction.set(globalTransaction);
    }
  }

  @Override
  public Snapshot snapshot() throws RestartStoreException {
    return delegate.snapshot();
  }

  @Override
  public Statistics getStatistics() {
    return delegate.getStatistics();
  }

  @Override
  public Future<Future<Snapshot>> pause() {
    return delegate.pause();
  }

  @Override
  public void resume() throws NotPausedException {
    delegate.resume();
  }

  public void commitControlledTransaction(I entityId) throws TransactionException {
    WrappedTransaction globalTransaction = getGlobalTransaction(entityId);
    if (activeTransaction.get() == null) {
      throw new IllegalStateException("Transaction is not started.");
    }
    try {
      if (globalTransaction == null) {
        activeTransaction.get().reallyCommit();
      }
    } finally {
      activeTransaction.remove();
    }
  }

  public void beginGlobalTransaction(boolean synchronous, I entityId) {
    if (entityId == null) {
      throw new UnsupportedOperationException("Entity name must be specified to start a global transaction");
    }
    WrappedTransaction globalTransaction = globalTransactionList.get(entityId);
    if (globalTransaction != null) {
      throw new UnsupportedOperationException("Nested global transactions are not supported.");
    }
    globalTransaction = globalTransactionList.putIfAbsent(entityId, new WrappedTransaction(synchronous));
    if (globalTransaction != null) {
      // for a given entity this is assumed to called under MANAGEMENT key..So give a warning out
      LOGGER.warn("Global Transaction exists for the entity. This is unexpected.");
    }
  }

  public void commitGlobalTransaction(I entityId) throws TransactionException {
    WrappedTransaction globalTransaction = getGlobalTransaction(entityId);
    if (globalTransaction == null) {
      throw new IllegalStateException("Global transaction is not started.");
    }
    try {
      globalTransaction.reallyCommit();
    } finally {
      globalTransactionList.remove(entityId);
    }
  }

  private WrappedTransaction getGlobalTransaction(I entityId) {
    return entityId != null ? globalTransactionList.get(entityId) : null;
  }

  private class WrappedTransaction implements Transaction<I, K, V> {

    private final Transaction<I, K, V> txn;
    private final boolean synchronous;

    private WrappedTransaction(boolean synchronous) {
      this.synchronous = synchronous;
      this.txn = delegate.beginTransaction(synchronous);
    }

    @Override
    public WrappedTransaction put(I id, K key, V value) throws TransactionException {
      txn.put(id, key, value);
      return this;
    }

    @Override
    public WrappedTransaction delete(I id) throws TransactionException {
      txn.delete(id);
      return this;
    }

    @Override
    public WrappedTransaction remove(I id, K key) throws TransactionException {
      txn.remove(id, key);
      return this;
    }

    @Override
    public void commit() {
      //
    }

    private void reallyCommit() throws TransactionException {
      txn.commit();
    }
  }
}
