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

package com.terracottatech.store.logic;

import com.terracottatech.store.Record;

import java.util.function.Predicate;

/**
 * Predicate testing records of a given type.
 * @param <K> type of record keys.
 */
public interface RecordPredicate<K extends Comparable<K>> extends Predicate<Record<K>> {

  /**
   * Whether the predicate is a contradiction.
   *
   * @return true if the predicate always evaluates to false, else false.
   */
  default boolean isContradiction() {
    return false;
  }

  /**
   * Whether the predicate is a tautology.
   *
   * @return true if the predicate always evaluates to true, else false.
   */

  default boolean isTautology() {
    return false;
  }
}
