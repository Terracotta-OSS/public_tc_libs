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
import java.util.function.Predicate;

import static com.terracottatech.store.UpdateOperation.install;
import static java.util.Arrays.asList;

/**
 * A ReadWriteRecordAccessor provides fine-control over read and write operations on
 * the record held against the key that was used to create the ReadWriteRecordAccessor.
 *
 * @param <K> the key type for the record
 */
public interface ReadWriteRecordAccessor<K extends Comparable<K>> extends ReadRecordAccessor<K> {
  /**
   * Read and write operations on the returned ConditionalReadWriteRecordAccessor will have the
   * supplied predicate applied to the record held against the key used to create this ReadWriteRecordAccessor.
   * If the predicate returns false, the operation will proceed as if there is no record against the key. If the
   * predicate returns true, the operation will proceed as normal.
   *
   * @param predicate the predicate to apply to the record held against the key used to create
   *                  this ReadWriteRecordAccessor.
   * @return A ConditionalReadWriteRecordAccessor that provides conditional read and write operations on the record
   * held against the key used to create this ReadWriteRecordAccessor. Any operations using this
   * ConditionalReadWriteRecordAccessor will have the supplied predicate applied to the record held against
   * the key and, if the predicate returns true, the record will be treated as present, otherwise any operation will act
   * as if there is no record against the key.
   */
  @Override
  ConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate);

  /**
   * Ensures that the record has the supplied cells.
   * If no record exists, a record is created. If there is an existing record then its cells are set to be those
   * supplied. Any cells that were on an existing record that were not in the set of cells supplied will no longer
   * be on the record at the time that the operation completes.
   *
   * @param cells the cells to ensure are on the record
   */
  default void upsert(Cell<?> ... cells) {
    upsert(asList(cells));
  }

  /**
   * Ensures that the record has the supplied cells.
   * If no record exists, a record is created. If there is an existing record then its cells are set to be those
   * supplied. Any cells that were on an existing record that were not in the set of cells supplied will no longer
   * be on the record at the time that the operation completes.
   *
   * @param cells a non-{@code null} {@code Iterable} supplying the cells to ensure are on the record
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  default void upsert(Iterable<Cell<?>> cells) {
    while (add(cells).isPresent() && !update(install(cells)).isPresent());
  }

  /**
   * Creates a record for the key that was used to create this ReadWriteRecordAccessor.
   * If a record is created an empty Optional is returned. If a record existed already, an Optional containing the
   * result of applying the supplied mapper function to the existing record is returned.
   *
   * @param mapper function to apply to the record held against the key used to create this ReadWriteRecordAccessor,
   *               if such a record existed already.
   * @param cells cells which form the record
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an empty Optional if the record was created, otherwise an Optional containing the result of applying the
   * function defined by the mapper parameter to the record that was already held against the key used to create this
   * ReadWriteRecordAccessor.
   */
  default <T> Optional<T> add(Function<? super Record<K>, T> mapper, Cell<?> ... cells) {
    return add(mapper, asList(cells));
  }

  /**
   * Creates a record for the key that was used to create this ReadWriteRecordAccessor.
   * If a record is created an empty Optional is returned. If a record existed already, an Optional containing the
   * result of applying the supplied mapper function to the existing record is returned.
   *
   * @param mapper function to apply to the record held against the key used to create this ReadWriteRecordAccessor,
   *               if such a record existed already.
   * @param cells a non-{@code null} {@code Iterable} supplying cells which form the record
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an empty Optional if the record was created, otherwise an Optional containing the result of applying the
   * function defined by the mapper parameter to the record that was already held against the key used to create this
   * ReadWriteRecordAccessor.
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  default <T> Optional<T> add(Function<? super Record<K>, T> mapper, Iterable<Cell<?>> cells) {
    return add(cells).map(mapper);
  }

  /**
   * Creates a record for the key used to create this ReadWriteRecordAccessor.
   * If a record is created an empty Optional is returned. If a record existed already, an Optional containing
   * the existing record is returned.
   *
   * @param cells cells which form the record
   * @return an empty Optional if the record was created, otherwise an Optional containing the record that was
   * already held against the key used to create this ReadWriteRecordAccessor.
   */
  default Optional<Record<K>> add(Cell<?> ... cells) {
    return add(asList(cells));
  }

  /**
   * Creates a record for the key used to create this ReadWriteRecordAccessor.
   * If a record is created an empty Optional is returned. If a record existed already, an Optional containing
   * the existing record is returned.
   *
   * @param cells a non-{@code null} {@code Iterable} supplying cells which form the record
   * @return an empty Optional if the record was created, otherwise an Optional containing the record that was
   * already held against the key used to create this ReadWriteRecordAccessor.
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  Optional<Record<K>> add(Iterable<Cell<?>> cells);

  /**
   * Updates a record held against the key used to create this ReadWriteRecordAccessor.
   * If the record is updated, an Optional containing the result of applying the mapper function is returned.
   * If no record to update was found, an empty Optional is returned.
   *
   * @param transform the mutating transformation to apply to the record
   * @param mapper the function to apply to the combination of the record that existed before the update
   *               and the record that resulted from the update. The first argument to the apply() method
   *               will be the record that existed before the update and the second argument will be the
   *               record that resulted from the update.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return if a record was held against the key, an Optional containing the result of applying the bimapper function
   * to the combination of the record that existed before the update and the record that resulted from the update,
   * otherwise, if there was no record against the key, an empty Optional
   */
  default <T> Optional<T> update(UpdateOperation<? super K> transform, BiFunction<? super Record<K>, ? super Record<K>, T> mapper) {
    return update(transform).map(t -> mapper.apply(t.getFirst(), t.getSecond()));
  }

  /**
   * Updates a record held against the key used to create this ReadWriteRecordAccessor.
   * If the record is updated, an Optional containing a Tuple containing the record that
   * existed before the update in the Tuple's first position and the record that resulted from
   * the update in the Tuple's second position, is returned.
   * If there was no record to update, an empty Optional is returned.
   *
   * @param transform the mutating transformation to apply to the record
   * @return if a record was held against the key, an Optional containing a Tuple containing the record that
   * existed before the update in the Tuple's first position and the record that resulted from
   * the update in the Tuple's second position, otherwise, if there was no record against the key, an empty Optional
   */
  Optional<Tuple<Record<K>, Record<K>>> update(UpdateOperation<? super K> transform);

  /**
   * Deletes a record held against the key used to create this ReadWriteRecordAccessor.
   * If the record is deleted, an Optional containing the result of applying the mapper function to the
   * deleted record is returned.
   * If there was no record held against the key, an empty Optional is returned.
   *
   * @param mapper function to apply to the deleted record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an Optional containing the result of applying the mapper function to the deleted record,
   * or an empty Optional if there was no record held against the key.
   */
  default <T> Optional<T> delete(Function<? super Record<K>, T> mapper) {
    return delete().map(mapper);
  }

  /**
   * Deletes a record held against the key used to create this ReadWriteRecordAccessor.
   * If the record is deleted, an Optional containing the deleted record is returned.
   * If there was no record held against the key, an empty Optional is returned.
   *
   * @return an Optional containing the record deleted, or an empty Optional if there was no record held against
   * the key.
   */
  Optional<Record<K>> delete();
}
