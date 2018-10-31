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
package com.terracottatech.store.async;

import com.terracottatech.store.Cell;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Arrays.asList;

/**
 * An asynchronous equivalent to {@link ReadWriteRecordAccessor}.
 */
public interface AsyncReadWriteRecordAccessor<K extends Comparable<K>> extends AsyncReadRecordAccessor<K> {
  /**
   * Equivalent to {@link ReadWriteRecordAccessor#iff(Predicate)}, but returns an
   * AsyncConditionalReadWriteRecordAccessor rather than a ConditionalReadWriteRecordAccessor.
   *
   * @param predicate the predicate to apply to the record held against the key used to
   *                  create this AsyncReadWriteRecordAccessor.
   * @return an AsyncConditionalReadWriteRecordAccessor
   */
  @Override
  AsyncConditionalReadWriteRecordAccessor<K> iff(Predicate<? super Record<K>> predicate);

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#upsert(Cell...)}.
   *
   * @param cells the cells to ensure are on the record
   * @return an operation representing the upsert execution
   */
  default Operation<Void> upsert(Cell<?> ... cells) {
    return upsert(asList(cells));
  }

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#upsert(Iterable)}.
   *
   * @param cells a non-{@code null} {@code Iterable} supplying the cells to ensure are on the record
   * @return an operation representing the upsert execution
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  Operation<Void> upsert(Iterable<Cell<?>> cells);

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#add(Function, Cell...)}.
   *
   * @param mapper function to apply to the record held against the key used to create this ReadWriteRecordAccessor,
   *               if such a record existed already.
   * @param cells cells which form the record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an operation representing the add.
   */
  default <T> Operation<Optional<T>> add(Function<? super Record<K>, T> mapper, Cell<?> ... cells) {
    return add(mapper, asList(cells));
  }

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#add(Function, Cell...)}.
   *
   * @param mapper function to apply to the record held against the key used to create this ReadWriteRecordAccessor,
   *               if such a record existed already.
   * @param cells a non-{@code null} {@code Iterable} supplying the cells which form the record
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an operation representing the add.
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  default <T> Operation<Optional<T>> add(Function<? super Record<K>, T> mapper, Iterable<Cell<?>> cells) {
    return add(cells).thenApplyAsync(oc -> oc.map(mapper));
  }

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#add(Cell...)}.
   *
   * @param cells cells which form the record.
   * @return an operation representing the add.
   */
  default Operation<Optional<Record<K>>> add(Cell<?> ... cells) {
    return add(asList(cells));
  }

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#add(Cell...)}.
   *
   * @param cells a non-{@code null} {@code Iterable} supplying the cells which form the record
   * @return an operation representing the add.
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   */
  Operation<Optional<Record<K>>> add(Iterable<Cell<?>> cells);

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#update(UpdateOperation, BiFunction)}.
   *
   * @param transform the mutating transformation to apply to the record
   * @param mapper the function to apply to the combination of the record that existed before the update
   *               and the record that resulted from the update. The first argument to the apply() method
   *               will be the record that existed before the update and the second argument will be the
   *               record that resulted from the update.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an operation representing the update.
   */
  default <T> Operation<Optional<T>> update(UpdateOperation<? super K> transform, BiFunction<? super Record<K>, ? super Record<K>, T> mapper) {
    return update(transform).thenApplyAsync(ou -> ou.map(t -> mapper.apply(t.getFirst(), t.getSecond())));
  }

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#update(UpdateOperation)}.
   *
   * @param transform the mutating transformation to apply to the record
   * @return an operation representing the update.
   */
  Operation<Optional<Tuple<Record<K>, Record<K>>>> update(UpdateOperation<? super K> transform);

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#delete(Function)}.
   *
   * @param mapper function to apply to the deleted record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an operation representing the delete.
   */
  default <T> Operation<Optional<T>> delete(Function<? super Record<K>, T> mapper) {
    return delete().thenApplyAsync(od -> od.map(mapper));
  }

  /**
   * An asynchronous equivalent to {@link ReadWriteRecordAccessor#delete()}.
   *
   * @return an operation representing the delete.
   */
  Operation<Optional<Record<K>>> delete();
}
