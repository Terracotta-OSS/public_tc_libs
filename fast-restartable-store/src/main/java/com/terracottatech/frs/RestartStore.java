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
package com.terracottatech.frs;

import com.terracottatech.frs.recovery.RecoveryException;

import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface RestartStore<I, K, V> {

  /**
   * Start the {@link RestartStore} for operation returning a {@link Future} representing the state of the
   * recovery process.
   *
   * @return {@link Future} that completes when recovery is completed.
   * @throws InterruptedException
   */
  Future<Void> startup() throws InterruptedException, RecoveryException;

  /**
   * Cleanly shut down the {@link RestartStore}. All in flight operations will be allowed
   * to finish, and their results will be flushed to stable storage.
   *
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException;

  /**
   * Open a transaction for mutating the {@link RestartStore}
   *
   * @param synchronous whether or not the transaction should be committed synchronously
   * @return a transaction context
   */
  Transaction<I, K, V> beginTransaction(boolean synchronous);

  /**
   * Open an auto-commit transaction.
   *
   * @param synchronous whether the actions within the autocommit transaction should be
   *                    committed synchronously
   * @return an auto-commit transaction context.
   */
  Transaction<I, K, V> beginAutoCommitTransaction(boolean synchronous);
  
  /**
   * randomly access a record from the log at a user provided marker
   *
   * @param marker the marker which was provided at put time
   * @return a tuple representing the action placed in the log, null if the action represented at
   *      the requested marker is not gettable or the marker does not exist in the log
   * 
   */
  Tuple<I, K, V> get(long marker);

  /**
   * Take a snapshot of this {@link RestartStore} for backup purposes. All transactions that have already been committed
   * prior to the snapshot call are guaranteed to be in the snapshot. Changes made while the snapshot is taken may or may
   * not be in the snapshot. The snapshot must be released after it's used in order to release any held resources.
   */
  Snapshot snapshot() throws RestartStoreException;
  
  /**
   * get statistics from the underlying implementation.  
   * 
   * 
   * @return statistics from current log stream implementation 
   */
  Statistics getStatistics();

  /**
   * Start the process of pausing incoming actions. Also, start the process of pausing compaction and return
   * a {@link Future} that can be used to check when the pause completes.
   *
   * The pause action is complete when all ongoing actions are queued (including compaction) and new actions
   * are frozen, thereby closing the gate for any future actions, until either a resume is called or until
   * an internal timeout happens. Just before completing the pause action, as snapshot request will be queued
   * and a future to this snapshot request will be returned.
   * <p>
   * A snapshot request takes a snapshot of this {@link RestartStore} for backup purposes. Once the snapshot completes
   * ({@link Future#get()} all transactions that have already been committed prior to the snapshot call are guaranteed
   * to be in the snapshot. Changes made while the snapshot is taken may or may not be in the snapshot. The snapshot
   * must be released after it's used in order to release any held resources and to unpause compaction.
   *
   * @return {@link Future} that completes when the pause is complete (including compaction pause and snapshot request
   *         queueing).
   */
  Future<Future<Snapshot>> pause();

  /**
   * Resume queueing of incoming actions for IO.
   *
   * @throws NotPausedException, if the store is NOT in a paused state.
   */
  void resume() throws NotPausedException;
}