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

import com.terracottatech.store.Record;

import java.util.function.Predicate;

/**
 * Testing a record inclusion in a query range and other ranges involved in the same query.
 * @param <K>
 */
public interface RangePredicate<K extends Comparable<K>> extends Predicate<Record<K>> {

  /**
   * Tests whether the record is included in other ranges involved in the same query and
   * hence its inclusion in query result should be deferred.
   * @param record
   * @return true if the record is included in at least one subsequent range included in the query.
   */
  default boolean overlaps(Record<K> record) {
    return false;
  }
}
