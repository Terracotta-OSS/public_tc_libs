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
package com.terracottatech.store.wrapper;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.common.ExceptionFreeAutoCloseable;
import com.terracottatech.store.indexing.Indexing;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class WrapperDataset<K extends Comparable<K>> implements Dataset<K>, ExceptionFreeAutoCloseable {

  private final SovereignDataset<K> backing;
  private final Indexing indexing;
  private final Executor asyncExecutor;
  private final Runnable closeTask;
  private final Supplier<Boolean> parentClosed;
  private final AtomicBoolean closed = new AtomicBoolean();

  public WrapperDataset(SovereignDataset<K> dataset, ExecutorService asyncExecutor, Runnable closeTask, Supplier<Boolean> parentClosed) {
    this.backing = dataset;
    this.closeTask = closeTask;
    this.asyncExecutor = asyncExecutor;
    this.parentClosed = parentClosed;
    this.indexing = new WrapperIndexing(dataset.getIndexing(), asyncExecutor, this::isClosed);
  }

  public SovereignDataset<K> getBacking() {
    return backing;
  }

  @Override
  public DatasetWriterReader<K> writerReader() {
    checkIfClosed();
    return new WrapperDatasetWriterReader<>(backing, asyncExecutor, SovereignDataset.Durability.LAZY);
  }

  @Override
  public DatasetReader<K> reader() {
    checkIfClosed();
    return new WrapperDatasetReader<>(backing, asyncExecutor);
  }

  @Override
  public Indexing getIndexing() {
    checkIfClosed();
    return indexing;
  }

  @Override
  public void close() {
    boolean closing = closed.compareAndSet(false, true);

    if (closing) {
      closeTask.run();
    }
  }

  private boolean isClosed() {
    return closed.get() || parentClosed.get();
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void finalize() {
    close();
  }

  private void checkIfClosed() {
    if (closed.get()) {
      throw new StoreRuntimeException("Attempt to use Dataset after close()");
    }

    if (parentClosed.get()) {
      throw new StoreRuntimeException("Attempt to use Dataset after its DatasetManager was closed");
    }
  }
}
