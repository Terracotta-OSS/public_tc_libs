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
package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.spi.store.Context;
import com.terracottatech.store.Record;
import com.terracottatech.store.internal.InternalRecord;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * A basic split iterator for records in sovereign
 *
 * @param <K> Record key type
 * @author mscott
 */
class RecordSpliterator<K extends Comparable<K>> implements Spliterator<Record<K>> {
  /**
   * The base {@code RecordStream} to which this spliterator is applied.  Although not of any operational
   * use, the value in this field prevents the stream from which an {@code Iterator} or {@code Spliterator}
   * is derived from being reclaimed by garbage collection.
   */
  protected final RecordStream<K> baseManagedStream;
  protected final long currentRevision;
  private PersistentMemoryLocator current;
  private final SovereignContainer<K> container;

  /**
   * Creates a {@code RecordSpliterator} using an externally obtained {@link Context}.
   *
   * @param baseManagedStream the base {@code RecordStream} for this spliterator
   * @param drive the {@code Locator} specifying the initial position for the spliterator
   * @param container the {@code DataContainer} through which the spliterator supplies records
   * @param currentRevision the current record MSN for the dataset
   */
  public RecordSpliterator(RecordStream<K> baseManagedStream, PersistentMemoryLocator drive, SovereignContainer<K> container, long currentRevision) {
    this.baseManagedStream = baseManagedStream;
    this.container = container;
    this.current = drive;
    this.currentRevision = currentRevision;
  }

  protected InternalRecord<K> next() {
    if (container.isDisposed()) {
      throw new IllegalStateException("Attempt to use disposed dataset");
    }
    while (!current.isEndpoint()) {
      SovereignPersistentRecord<K> data = container.get(current);
      try {
        if (data != null) {
          data.setLocation(current);
          return data;
        }
      } finally {
        current = current.next();
      }
    }
    return null;
  }

  /**
   * Seam for testing.
   * @return
   */
  public PersistentMemoryLocator getCurrent() {
    return current;
  }

  @Override
  public boolean tryAdvance(Consumer<? super Record<K>> action) {
    Record<K> next = next();
    if (next != null) {
      action.accept(next);
      return true;
    }
    return false;
  }

  @Override
  public Spliterator<Record<K>> trySplit() {
    return null;
  }

  @Override
  public int characteristics() {
    return 0;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  protected void skipTo(PersistentMemoryLocator nextLocation) {
    this.current = nextLocation;
  }
}
