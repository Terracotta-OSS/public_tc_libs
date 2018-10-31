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

import java.util.function.Supplier;

/**
 * Spliterator with capability to scan the index across multiple ranges until the {@link IndexRange}
 * instances are exhausted.
 *
 * @author RKAV
 */
class ManagedMultiScanSpliterator<K extends Comparable<K>> extends ManagedSpliterator<K> {
  private final Supplier<IndexRange<K>> locatorSupplier;

  ManagedMultiScanSpliterator(RecordStream<K> baseManagedStream,
                              SovereignContainer<K> container,
                              PersistentMemoryLocator firstLocator,
                              long currentRevision,
                              RangePredicate<K> currentEndpoint,
                              Supplier<IndexRange<K>> locatorSupplier,
                              ManagedAction<K> manager) {
    super(baseManagedStream, container, firstLocator, currentRevision, currentEndpoint, manager);
    this.locatorSupplier = locatorSupplier;
  }

  @Override
  protected boolean checkNextScan() {
    IndexRange<K> nextRange = locatorSupplier.get();
    if (nextRange != null) {
      skipTo(nextRange.getLocator(), nextRange.getEndpoint());
      return true;
    }
    return false;
  }
}
