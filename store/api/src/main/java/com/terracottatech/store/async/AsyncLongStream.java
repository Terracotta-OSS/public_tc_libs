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

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.LongStream;

/**
 * An asynchronous equivalent to {@link LongStream}.
 *
 * @author Chris Dennis
 * @see LongStream
 */
public interface AsyncLongStream extends BaseStream<Long, AsyncLongStream> {

  /**
   * An asynchronous equivalent to {@link LongStream#filter(java.util.function.LongPredicate)}.
   *
   * @param predicate the inclusion predicate
   * @return the new stream
   */
  AsyncLongStream filter(LongPredicate predicate);

  /**
   * An asynchronous equivalent to {@link LongStream#map(java.util.function.LongUnaryOperator)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncLongStream map(LongUnaryOperator mapper);

  /**
   * An asynchronous equivalent to {@link LongStream#mapToObj(java.util.function.LongFunction)}.
   *
   * @param <U> the element type of the new stream
   * @param mapper the element mapping function
   * @return the new stream
   */
  <U> AsyncStream<U> mapToObj(LongFunction<? extends U> mapper);

  /**
   * An asynchronous equivalent to {@link LongStream#mapToInt(java.util.function.LongToIntFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncIntStream mapToInt(LongToIntFunction mapper);

  /**
   * An asynchronous equivalent to {@link LongStream#mapToDouble(java.util.function.LongToDoubleFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncDoubleStream mapToDouble(LongToDoubleFunction mapper);

  /**
   * An asynchronous equivalent to {@link LongStream#flatMap(java.util.function.LongFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncLongStream flatMap(LongFunction<? extends LongStream> mapper);

  /**
   * An asynchronous equivalent to {@link LongStream#distinct()}.
   *
   * @return the new stream
   */
  AsyncLongStream distinct();

  /**
   * An asynchronous equivalent to {@link LongStream#sorted()}.
   *
   * @return the new stream
   */
  AsyncLongStream sorted();

  /**
   * An asynchronous equivalent to {@link LongStream#peek(java.util.function.LongConsumer)}.
   *
   * @param action action to perform on the elements as they are consumed
   * @return the new stream
   */
  AsyncLongStream peek(LongConsumer action);

  /**
   * An asynchronous equivalent to {@link LongStream#limit(long)}.
   *
   * @param maxSize maximum number of elements
   * @return the new stream
   */
  AsyncLongStream limit(long maxSize);

  /**
   * An asynchronous equivalent to {@link LongStream#skip(long)}.
   *
   * @param n number of leading elements to skip
   * @return the new stream
   */
  AsyncLongStream skip(long n);

  /**
   * An asynchronous equivalent to {@link LongStream#forEach(java.util.function.LongConsumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEach(LongConsumer action);

  /**
   * An asynchronous equivalent to {@link LongStream#forEachOrdered(java.util.function.LongConsumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEachOrdered(LongConsumer action);

  /**
   * An asynchronous equivalent to {@link LongStream#toArray()}.
   *
   * @return an {@code Operation} representing the conversion of this stream to an array
   */
  Operation<long[]> toArray();

  /**
   * An asynchronous equivalent to {@link LongStream#reduce(long, java.util.function.LongBinaryOperator)}.
   *
   * @param identity identity value of the accumulating operator
   * @param op the accumulating operator
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<Long> reduce(long identity, LongBinaryOperator op);

  /**
   * An asynchronous equivalent to {@link LongStream#reduce(java.util.function.LongBinaryOperator)}.
   *
   * @param op the accumulating operator
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<OptionalLong> reduce(LongBinaryOperator op);

  /**
   * An asynchronous equivalent to {@link LongStream#collect(java.util.function.Supplier, java.util.function.ObjLongConsumer, java.util.function.BiConsumer)}.
   *
   * @param <R> the type of the result
   * @param supplier the result container supplier
   * @param accumulator the accumulating function
   * @param combiner the combining function
   * @return an {@code Operation} representing the execution of this reduction
   */
  <R> Operation<R> collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner);

  /**
   * An asynchronous equivalent to {@link LongStream#sum()}.
   *
   * @return an {@code Operation} representing the sum of all elements
   */
  Operation<Long> sum();

  /**
   * An asynchronous equivalent to {@link LongStream#min()}.
   *
   * @return an {@code Operation} representing the calculation of the minimum value
   */
  Operation<OptionalLong> min();

  /**
   * An asynchronous equivalent to {@link LongStream#max()}.
   *
   * @return an {@code Operation} representing the calculation of the maximum value
   */
  Operation<OptionalLong> max();

  /**
   * An asynchronous equivalent to {@link LongStream#count()}.
   *
   * @return an {@code Operation} representing the length of the stream
   */
  Operation<Long> count();

  /**
   * An asynchronous equivalent to {@link LongStream#average()}.
   *
   * @return an {@code Operation} representing the calculation of the average of all elements
   */
  Operation<OptionalDouble> average();

  /**
   * An asynchronous equivalent to {@link LongStream#summaryStatistics()}.
   *
   * @return an {@code Operation} representing the calculation of statistics for this stream.
   */
  Operation<LongSummaryStatistics> summaryStatistics();

  /**
   * An asynchronous equivalent to {@link LongStream#anyMatch(java.util.function.LongPredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if at least one elements satisfies the predicate
   */
  Operation<Boolean> anyMatch(LongPredicate predicate);

  /**
   * An asynchronous equivalent to {@link LongStream#allMatch(java.util.function.LongPredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if all the elements satisfy the predicate
   */
  Operation<Boolean> allMatch(LongPredicate predicate);

  /**
   * An asynchronous equivalent to {@link LongStream#noneMatch(java.util.function.LongPredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if none of the elements satisfy the predicate
   */
  Operation<Boolean> noneMatch(LongPredicate predicate);

  /**
   * An asynchronous equivalent to {@link LongStream#findFirst()}.
   *
   * @return an {@code Operation} returning the first element of the stream
   */
  Operation<OptionalLong> findFirst();

  /**
   * An asynchronous equivalent to {@link LongStream#findAny()}.
   *
   * @return an {@code Operation} returning an element of the stream
   */
  Operation<OptionalLong> findAny();

  /**
   * An asynchronous equivalent to {@link LongStream#asDoubleStream()}.
   *
   * @return an {@code AsyncDoubleStream} containing these values converted to doubles
   */
  AsyncDoubleStream asDoubleStream();

  /**
   * An asynchronous equivalent to {@link LongStream#boxed()}.
   *
   * @return an {@code AsyncStream} containing boxed versions of this streams values
   */
  AsyncStream<Long> boxed();

  @Override
  AsyncLongStream sequential();

  @Override
  AsyncLongStream parallel();

  @Override
  PrimitiveIterator.OfLong iterator();

  @Override
  Spliterator.OfLong spliterator();
}
