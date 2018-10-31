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

import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An asynchronous equivalent to {@link ConditionalReadWriteRecordAccessor}.
 */
public interface AsyncConditionalReadWriteRecordAccessor<K extends Comparable<K>> extends AsyncConditionalReadRecordAccessor<K> {
  /**
   * An asynchronous equivalent to {@link ConditionalReadWriteRecordAccessor#update(UpdateOperation, BiFunction)}.
   *
   * @param transform the mutating transformation to apply to the record
   * @param bimapper the function to apply to the combination of the record that existed before the update
   *                 and the record that resulted from the update. The first argument to the apply() method
   *                 will be the record that existed before the update and the second argument will be the
   *                 record that resulted from the update.
   * @param <T> the type returned by the function defined in the bimapper parameter.
   * @return an Operation representing the update.
   */
  default <T> Operation<Optional<T>> update(UpdateOperation<? super K> transform, BiFunction<? super Record<K>, ? super Record<K>, T> bimapper) {
    return update(transform).thenApplyAsync(ou -> ou.map(t -> bimapper.apply(t.getFirst(), t.getSecond())));
  }

  /**
   * An asynchronous equivalent to {@link ConditionalReadWriteRecordAccessor#update(UpdateOperation)}.
   *
   * @param transform the mutating transformation to apply to the record
   * @return an Operation representing the update.
   */
  Operation<Optional<Tuple<Record<K>, Record<K>>>> update(UpdateOperation<? super K> transform);

  /**
   * An asynchronous equivalent to {@link ConditionalReadWriteRecordAccessor#delete(Function)}.
   *
   * @param mapper the function to apply to the deleted record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an Operation representing the delete.
   */
  default <T> Operation<Optional<T>>delete(Function<? super Record<K>, T> mapper) {
    return delete().thenApplyAsync(od -> od.map(mapper));
  }

  /**
   * An asynchronous equivalent to {@link ConditionalReadWriteRecordAccessor#delete()}.
   *
   * @return an Operation representing the delete.
   */
  Operation<Optional<Record<K>>> delete();
}
