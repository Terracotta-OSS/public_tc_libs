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
package com.terracottatech.sovereign.impl.model;

import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.spi.store.PersistentRecord;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.store.Cell;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 *
 * @param <K> the key type for this record
 *
 * @author cschanck
 */
public interface SovereignPersistentRecord<K extends Comparable<K>>
    extends PersistentRecord<K, PersistentMemoryLocator> {

  /**
   * Gets a read-only view of the {@code Cell} instances held by the first or only version of this
   * {@code SovereignPersistentRecord} indexed by cell name.
   *
   * @return an unmodifiable view of the {@code Cell} instances in this record
   */
  Map<String, Cell<?>> cells();

  /**
   * Gets the list of versioned record fragments used to compose this {@code SovereignPersistentRecord}.
   * If this record is not composed of versioned fragments, a {@code Collection} containing only {@code this}
   * is returned.
   *
   * @return the collection of {@code PersistentRecord} fragments of which this {@code PersistentRecord}
   * is formed
   */
  List<SovereignPersistentRecord<K>> elements();

  boolean deepEquals(SovereignPersistentRecord<K> that);

  void prune(TimeReference<?> z, BiFunction<TimeReference<?>, TimeReference<?>, VersionLimitStrategy.Retention> function);
}
