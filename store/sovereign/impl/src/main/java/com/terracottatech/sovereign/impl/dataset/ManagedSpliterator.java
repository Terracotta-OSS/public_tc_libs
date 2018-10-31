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
import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.store.Record;
import com.terracottatech.store.internal.InternalRecord;

import java.util.function.Consumer;

/**
 * @author mscott
 * @author RKAV modified to support TAB-5942 Index selection support for disjunction
 */
class ManagedSpliterator<K extends Comparable<K>>
    extends RecordSpliterator<K> implements AutoCloseable {
  private RangePredicate<K> endpoint;
  private final ManagedAction<K> manager;

  ManagedSpliterator(RecordStream<K> baseManagedStream, SovereignContainer<K> container,
                     PersistentMemoryLocator drive, long currentRevision,
                     RangePredicate<K> endpoint, ManagedAction<K> manager) {
    super(baseManagedStream, drive, container, currentRevision);
    this.endpoint = endpoint == null ? r -> true : endpoint;
    this.manager = manager == null ? () -> container : manager;
    if (this.manager.getContainer() != container) {
      throw new IllegalArgumentException(
        "mutation consumers must be created on the same dataset as the records stream");
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super Record<K>> action) {
    boolean cont = true;
    while (cont) {
      InternalRecord<K> next = next();
      if (next == null) {
        cont = checkNextScan();
      } else {
        next = manager.begin(next);
        try {
          cont = endpoint.test(next);
          if (!cont) {
            if (next.getMSN() > currentRevision) {
              cont = true;
            }
          }
          if (next != null && cont && !endpoint.overlaps(next)) {
            action.accept(next);
            return true;
          }
        } finally {
          manager.end(next);
        }
        if (!cont) {
          cont = checkNextScan();
        }
      }
    }
    return false;
  }

  @Override
  public void close() {
    if (this.manager instanceof AutoCloseable) {
      try {
        ((AutoCloseable) this.manager).close();
      } catch (Exception e) {
        // Ignored
      }
    }
  }

  protected boolean checkNextScan() {
    return false;
  }

  void skipTo(PersistentMemoryLocator nextLocator, RangePredicate<K> endpoint) {
    skipTo(nextLocator);
    this.endpoint = endpoint;
  }
}
