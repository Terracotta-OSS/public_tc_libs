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

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.IntStream;

/**
 * An asynchronous equivalent to {@link IntStream}.
 *
 * @author Chris Dennis
 * @see IntStream
 */
public interface AsyncIntStream extends BaseStream<Integer, AsyncIntStream> {

  /**
   * An asynchronous equivalent to {@link IntStream#filter(java.util.function.IntPredicate)}.
   *
   * @param predicate the inclusion predicate
   * @return the new stream
   */
  AsyncIntStream filter(IntPredicate predicate);

  /**
   * An asynchronous equivalent to {@link IntStream#map(java.util.function.IntUnaryOperator)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncIntStream map(IntUnaryOperator mapper);

  /**
   * An asynchronous equivalent to {@link IntStream#mapToObj(java.util.function.IntFunction)}.
   *
   * @param <U> the element type of the new stream
   * @param mapper the element mapping function
   * @return the new stream
   */
  <U> AsyncStream<U> mapToObj(IntFunction<? extends U> mapper);

  /**
   * An asynchronous equivalent to {@link IntStream#mapToLong(java.util.function.IntToLongFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncLongStream mapToLong(IntToLongFunction mapper);

  /**
   * An asynchronous equivalent to {@link IntStream#mapToDouble(java.util.function.IntToDoubleFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncDoubleStream mapToDouble(IntToDoubleFunction mapper);

  /**
   * An asynchronous equivalent to {@link IntStream#flatMap(java.util.function.IntFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncIntStream flatMap(IntFunction<? extends IntStream> mapper);

  /**
   * An asynchronous equivalent to {@link IntStream#distinct()}.
   *
   * @return the new stream
   */
  AsyncIntStream distinct();

  /**
   * An asynchronous equivalent to {@link IntStream#sorted()}.
   *
   * @return the new stream
   */
  AsyncIntStream sorted();

  /**
   * An asynchronous equivalent to {@link IntStream#peek(java.util.function.IntConsumer)}.
   *
   * @param action action to perform on the elements as they are consumed
   * @return the new stream
   */
  AsyncIntStream peek(IntConsumer action);

  /**
   * An asynchronous equivalent to {@link IntStream#limit(long)}.
   *
   * @param maxSize maximum number of elements
   * @return the new stream
   */
  AsyncIntStream limit(long maxSize);

  /**
   * An asynchronous equivalent to {@link IntStream#skip(long)}.
   *
   * @param n number of leading elements to skip
   * @return the new stream
   */
  AsyncIntStream skip(long n);

  /**
   * An asynchronous equivalent to {@link IntStream#forEach(java.util.function.IntConsumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEach(IntConsumer action);

  /**
   * An asynchronous equivalent to {@link IntStream#forEachOrdered(java.util.function.IntConsumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEachOrdered(IntConsumer action);

  /**
   * An asynchronous equivalent to {@link IntStream#toArray()}.
   *
   * @return an {@code Operation} representing the conversion of this stream to an array
   */
  Operation<int[]> toArray();

  /**
   * An asynchronous equivalent to {@link IntStream#reduce(int, java.util.function.IntBinaryOperator)}.
   *
   * @param identity identity value of the accumulating operator
   * @param op the accumulating operator
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<Integer> reduce(int identity, IntBinaryOperator op);

  /**
   * An asynchronous equivalent to {@link IntStream#reduce(java.util.function.IntBinaryOperator)}.
   *
   * @param op the accumulating operator
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<OptionalInt> reduce(IntBinaryOperator op);

  /**
   * An asynchronous equivalent to {@link IntStream#collect(java.util.function.Supplier, java.util.function.ObjIntConsumer, java.util.function.BiConsumer)}.
   *
   * @param <R> the type of the result
   * @param supplier the result container supplier
   * @param accumulator the accumulating function
   * @param combiner the combining function
   * @return an {@code Operation} representing the execution of this reduction
   */
  <R> Operation<R> collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner);

  /**
   * An asynchronous equivalent to {@link IntStream#sum()}.
   *
   * @return an {@code Operation} representing the sum of all elements
   */
  Operation<Integer> sum();

  /**
   * An asynchronous equivalent to {@link IntStream#min()}.
   *
   * @return an {@code Operation} representing the calculation of the minimum value
   */
  Operation<OptionalInt> min();

  /**
   * An asynchronous equivalent to {@link IntStream#max()}.
   *
   * @return an {@code Operation} representing the calculation of the maximum value
   */
  Operation<OptionalInt> max();

  /**
   * An asynchronous equivalent to {@link IntStream#count()}.
   *
   * @return an {@code Operation} representing the length of the stream
   */
  Operation<Long> count();

  /**
   * An asynchronous equivalent to {@link IntStream#average()}.
   *
   * @return an {@code Operation} representing the calculation of the average of all elements
   */
  Operation<OptionalDouble> average();

  /**
   * An asynchronous equivalent to {@link IntStream#summaryStatistics()}.
   *
   * @return an {@code Operation} representing the calculation of statistics for this stream.
   */
  Operation<IntSummaryStatistics> summaryStatistics();

  /**
   * An asynchronous equivalent to {@link IntStream#anyMatch(java.util.function.IntPredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if at least one elements satisfies the predicate
   */
  Operation<Boolean> anyMatch(IntPredicate predicate);

  /**
   * An asynchronous equivalent to {@link IntStream#allMatch(java.util.function.IntPredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if all the elements satisfy the predicate
   */
  Operation<Boolean> allMatch(IntPredicate predicate);

  /**
   * An asynchronous equivalent to {@link IntStream#noneMatch(java.util.function.IntPredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if none of the elements satisfy the predicate
   */
  Operation<Boolean> noneMatch(IntPredicate predicate);

  /**
   * An asynchronous equivalent to {@link IntStream#findFirst()}.
   *
   * @return an {@code Operation} returning the first element of the stream
   */
  Operation<OptionalInt> findFirst();

  /**
   * An asynchronous equivalent to {@link IntStream#findAny()}.
   *
   * @return an {@code Operation} returning an element of the stream
   */
  Operation<OptionalInt> findAny();

  /**
   * An asynchronous equivalent to {@link IntStream#asLongStream()}.
   *
   * @return an {@code AsyncLongStream} containing these values converted to longs
   */
  AsyncLongStream asLongStream();

  /**
   * An asynchronous equivalent to {@link IntStream#asDoubleStream()}.
   *
   * @return an {@code AsyncDoubleStream} containing these values converted to doubles
   */
  AsyncDoubleStream asDoubleStream();

  /**
   * An asynchronous equivalent to {@link IntStream#boxed()}.
   *
   * @return an {@code AsyncStream} containing boxed versions of this streams values
   */
  AsyncStream<Integer> boxed();

  @Override
  AsyncIntStream sequential();

  @Override
  AsyncIntStream parallel();

  @Override
  PrimitiveIterator.OfInt iterator();

  @Override
  Spliterator.OfInt spliterator();
}
