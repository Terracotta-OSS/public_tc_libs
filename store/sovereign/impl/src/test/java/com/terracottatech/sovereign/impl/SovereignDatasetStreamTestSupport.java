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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.exceptions.LockConflictException;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.tool.Diagnostics;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;

/**
 * @author Clifford W. Johnson
 */
public class SovereignDatasetStreamTestSupport {
  private SovereignDatasetStreamTestSupport() {
  }

  /**
   * Method returning a dataset record consumer that performs an update under lock.  The consumer is
   * <b>intentionally not</b> a {@link ManagedAction} which is disallowed for {@code peek}.
   * <p>
   * When used in a {@code peek} operation in a mutative pipeline, this {@code Consumer} is expected to fail to update
   * the record.  (Internally, the update operation fails with a {@code LockConflictException} which is signaled by
   * setting the {@code timedOut} parameter to {@code true}.)
   *
   * @param dataset the {@code SovereignDataset} against which the mutation is performed
   * @param targetKey the key value for which the update is to activate
   * @param updated an {@code AtomicBoolean} set to {@code true} if the update succeeds
   * @param timedOut an {@code AtomicBoolean} set to {@code true} if the update operation times out
   *
   * @return the updating {@code Consumer}
   */
  public static <K extends Comparable<K>>
  Consumer<Record<String>> lockingConsumer(SovereignDataset<K> dataset, K targetKey, AtomicBoolean updated, AtomicBoolean timedOut) {
    long lockTimeout = ((SovereignDatasetImpl)dataset).getConfig().getRecordLockTimeout();
    return r -> {
      Semaphore syncPoint = new Semaphore(0);
      if (r.getKey().equals(targetKey)) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          syncPoint.release();
          dataset.applyMutation(SovereignDataset.Durability.IMMEDIATE,
              targetKey,
              (record) -> true,
              (record) -> {
                CellSet cells = new CellSet(record);
                cells.set(OBSERVATIONS.newCell(1 + cells.get(OBSERVATIONS).orElse(0L)));
                return cells;
              });
          updated.set(true);
        });
        try {
          syncPoint.acquire();
          // Wait at least as long as the record lock timeout period to permit a LockConflictException
          future.get((100L + lockTimeout) * 2, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
          if (e.getCause() instanceof LockConflictException) {
            // Expected
            timedOut.set(true);
          } else {
            throw new AssertionError(e);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
          Diagnostics.threadDump();
          throw new AssertionError(e);
        }
      }
    };
  }
}
