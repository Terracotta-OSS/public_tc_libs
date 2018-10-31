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
import java.util.function.Predicate;

/**
 * A ReadRecordAccessor provides fine-control over read operations on
 * the record held against the key that was used to create the ReadRecordAccessor.
 *
 * @param <K> the key type for the record
 */
public interface ReadRecordAccessor<K extends Comparable<K>> {
  /**
   * Read operations on the returned ConditionalReadRecordAccessor will have the
   * supplied predicate applied to the record held against the key used to create this ReadRecordAccessor.
   * If the predicate returns false, the read will proceed as if there is no record against the key. If the
   * predicate returns true, the read will proceed as normal.
   *
   * @param predicate the predicate to apply to the record held against the key used to create
   *                  this ReadRecordAccessor.
   * @return A ConditionalReadRecordAccessor that provides conditional read operations on the record
   * held against the key used to create this ReadRecordAccessor. Any reads using this
   * ConditionalReadRecordAccessor will have the supplied predicate applied to the record held against
   * the key and, if the predicate returns true, the record will be read, otherwise any read will act
   * as if there is no record against the key.
   */
  ConditionalReadRecordAccessor<K> iff(Predicate<? super Record<K>> predicate);

  /**
   * Maps the record held against the key used to create this ReadRecordAccessor. The record is
   * mapped by applying the supplied mapper function and the resulting value is returned.
   *
   * @param mapper the function to apply to the record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an Optional containing the result of applying the function defined in
   * the mapper parameter to the record held against the key if it is present,
   * otherwise an empty Optional.
   */
  <T> Optional<T> read(Function<? super Record<K>, T> mapper);
}
