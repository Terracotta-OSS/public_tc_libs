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
package com.terracottatech.store;

import java.util.Optional;
import java.util.function.Function;

/**
 * A ConditionalReadRecordAccessor provides read operations on a record.
 * However, those operations are conditional on the predicate that was used to create the
 * ConditionalReadRecordAccessor.
 *
 * @param <K> the key type for the record
 */
public interface ConditionalReadRecordAccessor<K extends Comparable<K>> {
  /**
   * Maps the record held against the key used to create this ConditionalReadRecordAccessor,
   * if that record matches the predicate used to create this ConditionalReadRecordAccessor.
   * The record is mapped using the supplied mapper function and the resulting value is returned.
   *
   * @param mapper the function to apply to the record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an Optional containing the result of applying the function defined in
   * the mapper parameter to the record held against the key if it is present
   * and if it also matches the predicate that was used to create this
   * ConditionalReadRecordAccessor, otherwise an empty Optional.
   */
  default <T> Optional<T> read(Function<? super Record<K>, T> mapper) {
    return read().map(mapper);
  }

  /**
   * Reads the record held against the key used to create this ConditionalReadRecordAccessor,
   * if that record matches the predicate used to create this ConditionalReadRecordAccessor.
   *
   * @return an Optional containing the record held against the key if it is present
   * and if it also matches the predicate that was used to create this
   * ConditionalReadRecordAccessor, otherwise an empty Optional.
   */
  Optional<Record<K>> read();
}
