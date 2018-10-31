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

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;

/**
 * An asynchronous equivalent to {@link DoubleStream}.
 *
 * @author Chris Dennis
 * @see DoubleStream
 */
public interface AsyncDoubleStream extends BaseStream<Double, AsyncDoubleStream> {

  /**
   * An asynchronous equivalent to {@link DoubleStream#filter(java.util.function.DoublePredicate)}.
   *
   * @param predicate the inclusion predicate
   * @return the new stream
   */
  AsyncDoubleStream filter(DoublePredicate predicate);

  /**
   * An asynchronous equivalent to {@link DoubleStream#map(java.util.function.DoubleUnaryOperator)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncDoubleStream map(DoubleUnaryOperator mapper);

  /**
   * An asynchronous equivalent to {@link DoubleStream#mapToObj(java.util.function.DoubleFunction)}.
   *
   * @param <U> the element type of the new stream
   * @param mapper the element mapping function
   * @return the new stream
   */
  <U> AsyncStream<U> mapToObj(DoubleFunction<? extends U> mapper);

  /**
   * An asynchronous equivalent to {@link DoubleStream#mapToInt(java.util.function.DoubleToIntFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncIntStream mapToInt(DoubleToIntFunction mapper);

  /**
   * An asynchronous equivalent to {@link DoubleStream#mapToLong(java.util.function.DoubleToLongFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncLongStream mapToLong(DoubleToLongFunction mapper);

  /**
   * An asynchronous equivalent to {@link DoubleStream#flatMap(java.util.function.DoubleFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncDoubleStream flatMap(DoubleFunction<? extends DoubleStream> mapper);

  /**
   * An asynchronous equivalent to {@link DoubleStream#distinct()}.
   *
   * @return the new stream
   */
  AsyncDoubleStream distinct();

  /**
   * An asynchronous equivalent to {@link DoubleStream#sorted()}.
   *
   * @return the new stream
   */
  AsyncDoubleStream sorted();

  /**
   * An asynchronous equivalent to {@link DoubleStream#peek(java.util.function.DoubleConsumer)}.
   *
   * @param action action to perform on the elements as they are consumed
   * @return the new stream
   */
  AsyncDoubleStream peek(DoubleConsumer action);

  /**
   * An asynchronous equivalent to {@link DoubleStream#limit(long)}.
   *
   * @param maxSize maximum number of elements
   * @return the new stream
   */
  AsyncDoubleStream limit(long maxSize);

  /**
   * An asynchronous equivalent to {@link DoubleStream#skip(long)}.
   *
   * @param n number of leading elements to skip
   * @return the new stream
   */
  AsyncDoubleStream skip(long n);

  /**
   * An asynchronous equivalent to {@link DoubleStream#forEach(java.util.function.DoubleConsumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEach(DoubleConsumer action);

  /**
   * An asynchronous equivalent to {@link DoubleStream#forEachOrdered(java.util.function.DoubleConsumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEachOrdered(DoubleConsumer action);

  /**
   * An asynchronous equivalent to {@link DoubleStream#toArray()}.
   *
   * @return an {@code Operation} representing the conversion of this stream to an array
   */
  Operation<double[]> toArray();

  /**
   * An asynchronous equivalent to {@link DoubleStream#reduce(double, java.util.function.DoubleBinaryOperator)}.
   *
   * @param identity identity value of the accumulating operator
   * @param op the accumulating operator
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<Double> reduce(double identity, DoubleBinaryOperator op);

  /**
   * An asynchronous equivalent to {@link DoubleStream#reduce(java.util.function.DoubleBinaryOperator)}.
   *
   * @param op the accumulating operator
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<OptionalDouble> reduce(DoubleBinaryOperator op);

  /**
   * An asynchronous equivalent to {@link DoubleStream#collect(java.util.function.Supplier, java.util.function.ObjDoubleConsumer, java.util.function.BiConsumer)}.
   *
   * @param <R> the type of the result
   * @param supplier the result container supplier
   * @param accumulator the accumulating function
   * @param combiner the combining function
   * @return an {@code Operation} representing the execution of this reduction
   */
  <R> Operation<R> collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner);

  /**
   * An asynchronous equivalent to {@link DoubleStream#sum()}.
   *
   * @return an {@code Operation} representing the sum of all elements
   */
  Operation<Double> sum();

  /**
   * An asynchronous equivalent to {@link DoubleStream#min()}.
   *
   * @return an {@code Operation} representing the calculation of the minimum value
   */
  Operation<OptionalDouble> min();

  /**
   * An asynchronous equivalent to {@link DoubleStream#max()}.
   *
   * @return an {@code Operation} representing the calculation of the maximum value
   */
  Operation<OptionalDouble> max();

  /**
   * An asynchronous equivalent to {@link DoubleStream#count()}.
   *
   * @return an {@code Operation} representing the length of the stream
   */
  Operation<Long> count();

  /**
   * An asynchronous equivalent to {@link DoubleStream#average()}.
   *
   * @return an {@code Operation} representing the calculation of the average of all elements
   */
  Operation<OptionalDouble> average();

  /**
   * An asynchronous equivalent to {@link DoubleStream#summaryStatistics()}.
   *
   * @return an {@code Operation} representing the calculation of statistics for this stream.
   */
  Operation<DoubleSummaryStatistics> summaryStatistics();

  /**
   * An asynchronous equivalent to {@link DoubleStream#anyMatch(java.util.function.DoublePredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if at least one elements satisfies the predicate
   */
  Operation<Boolean> anyMatch(DoublePredicate predicate);

  /**
   * An asynchronous equivalent to {@link DoubleStream#allMatch(java.util.function.DoublePredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if all the elements satisfy the predicate
   */
  Operation<Boolean> allMatch(DoublePredicate predicate);

  /**
   * An asynchronous equivalent to {@link DoubleStream#noneMatch(java.util.function.DoublePredicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if none of the elements satisfy the predicate
   */
  Operation<Boolean> noneMatch(DoublePredicate predicate);

  /**
   * An asynchronous equivalent to {@link DoubleStream#findFirst()}.
   *
   * @return an {@code Operation} returning the first element of the stream
   */
  Operation<OptionalDouble> findFirst();

  /**
   * An asynchronous equivalent to {@link DoubleStream#findAny()}.
   *
   * @return an {@code Operation} returning an element of the stream
   */
  Operation<OptionalDouble> findAny();

  /**
   * An asynchronous equivalent to {@link DoubleStream#boxed()}.
   *
   * @return an {@code AsyncStream} containing boxed versions of this streams values
   */
  AsyncStream<Double> boxed();

  @Override
  AsyncDoubleStream sequential();

  @Override
  AsyncDoubleStream parallel();

  @Override
  PrimitiveIterator.OfDouble iterator();

  @Override
  Spliterator.OfDouble spliterator();
}
