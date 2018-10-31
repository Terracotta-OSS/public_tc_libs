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

import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An asynchronous equivalent to {@link ReadRecordAccessor}.
 */
public interface AsyncReadRecordAccessor<K extends Comparable<K>> {
  /**
   * Equivalent to {@link ReadRecordAccessor#iff(Predicate)}, but returns an AsyncConditionalReadRecordAccessor
   * rather than a ConditionalReadRecordAccessor.
   *
   * @param predicate the predicate to apply to the record held against the key used to
   *                  create this AsyncReadRecordAccessor.
   * @return an AsyncConditionalReadRecordAccessor
   */
  AsyncConditionalReadRecordAccessor<K> iff(Predicate<? super Record<K>> predicate);

  /**
   * An asynchronous equivalent to {@link ReadRecordAccessor#read(Function)}.
   *
   * @param mapper the function to apply to the record.
   * @param <T> the type returned by the function defined in the mapper parameter.
   * @return an operation representing the read.
   */
  <T> Operation<Optional<T>> read(Function<? super Record<K>, T> mapper);
}
