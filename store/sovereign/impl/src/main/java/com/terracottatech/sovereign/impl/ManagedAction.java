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

import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.store.internal.InternalRecord;

/**
 * Defines a bounded action.  When provided to a {@code Spliterator} used by
 * {@link com.terracottatech.sovereign.RecordStream},
 * presentation of an {@code InternalRecord} to the pipeline starts with a call to
 * {@link #begin(InternalRecord) begin} and is finished by a call to {@link #end(InternalRecord) end}.
 * <p>
 * It is possible for a single {@code ManagedAction} implementation instance to be
 * shared among multiple streams and multiple threads in a parallel {@code Stream}
 * pipeline.  Each implementation is responsible for managing thread-safety.
 *
 * @author mscott
 *
 * @see com.terracottatech.sovereign.impl.dataset.ManagedSpliterator
 * @see com.terracottatech.sovereign.impl.dataset.PassThroughManagedSpliterator
 */
public interface ManagedAction<K extends Comparable<K>> {

  /**
   * Prepares an {@link InternalRecord} for processing by this {@code ManagedAction}.
   * <p>
   * The default implementation returns simply returns {@code record}.
   *
   * @param record the {@code InternalRecord} to begin processing
   *
   * @return the {@code InternalRecord} instance to provide to the {@link #end(InternalRecord) end} method;
   *      the returned instance <i>may</i> not be {@code record}
   */
  default InternalRecord<K> begin(InternalRecord<K> record) {
    return record;
  }

  /**
   * Finalize processing for a {@code Record}.
   * <p>
   * The default implementation takes no action.
   *
   * @param record the {@code Record} instance returned by the {@link #begin(InternalRecord) begin} method
   */
  default void end(InternalRecord<K> record) {
  }

  SovereignContainer<K> getContainer();
}
