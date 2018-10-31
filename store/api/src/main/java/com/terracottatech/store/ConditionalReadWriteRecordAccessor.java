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
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A ConditionalReadWriteRecordAccessor provides read and write transformations on a record.
 * However, those transformations are conditional on the predicate that was used to create the
 * ConditionalReadWriteRecordAccessor.
 *
 * @param <K> the key type for the record
 */
public interface ConditionalReadWriteRecordAccessor<K extends Comparable<K>> extends ConditionalReadRecordAccessor<K> {
  /**
   * Updates the record held against the key used to create this ConditionalReadWriteRecordAccessor,
   * if that record matches the predicate used to create this ConditionalReadWriteRecordAccessor.
   * The the function defined in the bimapper parameter is applied to the combination of the record
   * that existed before the update and the record that resulted from the update and the result of this is returned.
   *
   * @param transform the mutating transform to apply to the record
   * @param bimapper the function to apply to the combination of the record that existed before the update
   *                 and the record that resulted from the update. The first argument to the apply() method
   *                 will be the record that existed before the update and the second argument will be the
   *                 record that resulted from the update.
   * @param <T> the type returned by the function defined in the bimapper parameter.
   * @return an Optional containing the mapped result of applying the mutating transform to
   * the record held against the key if it is present and if it also matches the predicate
   * that was used to create this ConditionalReadWriteRecordAccessor, otherwise an empty Optional.
   * If the Optional returned is not empty then it will contain the result of the application of the function
   * defined in the bimapper parameter.
   */
  default <T> Optional<T> update(UpdateOperation<? super K> transform, BiFunction<? super Record<K>, ? super Record<K>, T> bimapper) {
    return update(transform).map(t -> bimapper.apply(t.getFirst(), t.getSecond()));
  }

  /**
   * Updates the record held against the key used to create this ConditionalReadWriteRecordAccessor,
   * if that record matches the predicate used to create this ConditionalReadWriteRecordAccessor.
   *
   * @param transform the mutating transform to apply to the record
   * @return An Optional containing the result of applying the mutating transform to
   * the record held against the key if it is present and if it also matches the predicate
   * that was used to create this ConditionalReadWriteRecordAccessor, otherwise an empty Optional.
   * If the Optional returned is not empty then it will contain a Tuple containing the record that
   * existed before the update in the Tuple's first position and the record that resulted from
   * the update in the Tuple's second position.
   */
  Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform);

  /**
   * Deletes the record held against the key used to create this ConditionalReadWriteRecordAccessor,
   * if that record matches the predicate used to create this ConditionalReadWriteRecordAccessor.
   * The the function defined in the mapper parameter is applied to the deleted record and the result
   * of this is returned.
   *
   * @param mapper the function to apply to the deleted record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return An Optional containing the result of applying the function defined in the mapper parameter
   * to the deleted record if a record was actually deleted, otherwise an empty Optional.
   */
  default <T> Optional<T> delete(Function<? super Record<K>, T> mapper) {
    return delete().map(mapper);
  }

  /**
   * Deletes the record held against the key used to create this ConditionalReadWriteRecordAccessor,
   * if that record matches the predicate used to create this ConditionalReadWriteRecordAccessor.
   *
   * @return An Optional containing the deleted record if a record was actually deleted,
   * otherwise an empty Optional.
   */
  Optional<Record<K>> delete();
}
