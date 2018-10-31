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
package com.terracottatech.sovereign.btrees.bplustree;

import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.bplustree.appendonly.GarbageCollectionQueue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * The AccessManager. Used to bring together the garbage collecion object, revision
 * management, and the active set.
 *
 * @author cschanck
 */
public class AccessManager {
  private final AtomicLong currentRevision = new AtomicLong(0);
  private final ListBasedAccessTracker activeSet;
  private final GarbageCollectionQueue gcQueue;
  private final LongConsumer notifyTask;

  /**
   * Instantiates a new Access manager.
   * @param gcTask the gc task
   * @param gcMinimum the gc minimum
   */
  public AccessManager(GarbageCollectionQueue.GCTask gcTask, LongConsumer notifyTask, int gcMinimum) {
    this.activeSet = new StripedListBasedAccessTrackerImpl();
    this.notifyTask = notifyTask == null ? (rev) -> {
    } : notifyTask;
    this.gcQueue = new GarbageCollectionQueue(gcTask, gcMinimum);
  }

  public void queueFree(SimpleStore s, final long revision, long addr) {
    gcQueue.queueFree(s, revision, addr);
  }

  public void garbageCollect(long ceilingRevision) throws IOException {
    long ceiling = activeSet.getLowestActiveRevision(ceilingRevision);
    gcQueue.garbageCollect(ceiling);
    notifyTask.accept(ceiling);
  }

  public ReadAccessor assign(Tx<?> tx) {
    return activeSet.registerRead(tx, tx.getWorkingRevision());
  }

  public void release(ReadAccessor accessor) {
    activeSet.deregisterRead(accessor);
  }

  public long nextRevisionNumber() {
    return currentRevision.incrementAndGet();
  }

  /**
   * Gets current revision number.
   *
   * @return the current revision number
   */
  public long getCurrentRevisionNumber() {
    return currentRevision.get();
  }

  /**
   * Sets current revision number.
   *
   * @param r the r
   */
  public void setCurrentRevisionNumber(long r) {
    currentRevision.set(r);
  }

  public void invalidateRevision(long revision) {
    gcQueue.invalidateRevision(revision);
  }
}
