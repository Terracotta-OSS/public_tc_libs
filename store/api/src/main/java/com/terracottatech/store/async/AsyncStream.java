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

import java.util.Comparator;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * An asynchronous equivalent to {@link Stream}.
 *
 * @author Chris Dennis
 * @param <T> the type of the stream elements
 * @see Stream
 */
public interface AsyncStream<T> extends BaseStream<T, AsyncStream<T>> {

  /**
   * An asynchronous equivalent to {@link Stream#filter(java.util.function.Predicate)}.
   *
   * @param predicate the inclusion predicate
   * @return the new stream
   */
  AsyncStream<T> filter(Predicate<? super T> predicate);

  /**
   * An asynchronous equivalent to {@link Stream#map(java.util.function.Function)}.
   *
   * @param <R> the element type of the new stream
   * @param mapper the element mapping function
   * @return the new stream
   */
  <R> AsyncStream<R> map(Function<? super T, ? extends R> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#mapToInt(java.util.function.ToIntFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncIntStream mapToInt(ToIntFunction<? super T> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#mapToLong(java.util.function.ToLongFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncLongStream mapToLong(ToLongFunction<? super T> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#mapToDouble(java.util.function.ToDoubleFunction)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncDoubleStream mapToDouble(ToDoubleFunction<? super T> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#flatMap(java.util.function.Function)}.
   *
   * @param <R> the element type of the new stream
   * @param mapper the element mapping function
   * @return the new stream
   */
  <R> AsyncStream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#flatMapToInt(java.util.function.Function)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncIntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#flatMapToLong(java.util.function.Function)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncLongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#flatMapToDouble(java.util.function.Function)}.
   *
   * @param mapper the element mapping function
   * @return the new stream
   */
  AsyncDoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper);

  /**
   * An asynchronous equivalent to {@link Stream#distinct()}.
   *
   * @return the new stream
   */
  AsyncStream<T> distinct();

  /**
   * An asynchronous equivalent to {@link Stream#sorted()}.
   *
   * @return the new stream
   */
  AsyncStream<T> sorted();

  /**
   * An asynchronous equivalent to {@link Stream#sorted(java.util.Comparator)}.
   *
   * @param comparator comparator used to compare the elements
   * @return the new stream
   */
  AsyncStream<T> sorted(Comparator<? super T> comparator);

  /**
   * An asynchronous equivalent to {@link Stream#peek(java.util.function.Consumer)}.
   *
   * @param action action to perform on the elements as they are consumed
   * @return the new stream
   */
  AsyncStream<T> peek(Consumer<? super T> action);

  /**
   * An asynchronous equivalent to {@link Stream#limit(long)}.
   *
   * @param maxSize maximum number of elements
   * @return the new stream
   */
  AsyncStream<T> limit(long maxSize);

  /**
   * An asynchronous equivalent to {@link Stream#skip(long)}.
   *
   * @param n number of leading elements to skip
   * @return the new stream
   */
  AsyncStream<T> skip(long n);

  /**
   * An asynchronous equivalent to {@link Stream#forEach(java.util.function.Consumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEach(Consumer<? super T> action);

  /**
   * An asynchronous equivalent to {@link Stream#forEachOrdered(java.util.function.Consumer)}.
   *
   * @param action action to perform on the elements
   * @return an {@code Operation} representing the completion of this action across all elements
   */
  Operation<Void> forEachOrdered(Consumer<? super T> action);

  /**
   * An asynchronous equivalent to {@link Stream#toArray()}.
   *
   * @return an {@code Operation} representing the conversion of this stream to an array
   */
  Operation<Object[]> toArray();

  /**
   * An asynchronous equivalent to {@link Stream#toArray(java.util.function.IntFunction)}.
   *
   * @param <A> the element type of the array
   * @param generator a generator function for an array of the desired type and length
   * @return an {@code Operation} representing the conversion of this stream to an array
   */
  <A> Operation<A[]> toArray(IntFunction<A[]> generator);

  /**
   * An asynchronous equivalent to {@link Stream#reduce(java.lang.Object, java.util.function.BinaryOperator)}.
   *
   * @param identity identity value of the accumulating function
   * @param accumulator the accumulating function
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<T> reduce(T identity, BinaryOperator<T> accumulator);

  /**
   * An asynchronous equivalent to {@link Stream#reduce(java.util.function.BinaryOperator)}.
   *
   * @param accumulator the accumulating function
   * @return an {@code Operation} representing the execution of this reduction
   */
  Operation<Optional<T>> reduce(BinaryOperator<T> accumulator);

  /**
   * An asynchronous equivalent to {@link Stream#reduce(java.lang.Object, java.util.function.BiFunction, java.util.function.BinaryOperator)}.
   *
   * @param <U> the type of the result
   * @param identity identity value of the accumulating function
   * @param accumulator the accumulating function
   * @param combiner the combining function
   * @return an {@code Operation} representing the execution of this reduction
   */
  <U> Operation<U> reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner);

  /**
   * An asynchronous equivalent to {@link Stream#collect(java.util.function.Supplier, java.util.function.BiConsumer, java.util.function.BiConsumer)}.
   *
   * @param <R> the type of the result
   * @param supplier the result container supplier
   * @param accumulator the accumulating function
   * @param combiner the combining function
   * @return an {@code Operation} representing the execution of this reduction
   */
  <R> Operation<R> collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner);

  /**
   * An asynchronous equivalent to {@link Stream#reduce(java.util.function.BinaryOperator)}.
   *
   * @param <R> the type of the result
   * @param <A> the intermediate accumulation type
   * @param collector the {@code Collector} describing the operation
   * @return an {@code Operation} representing the execution of this reduction
   */
  <R, A> Operation<R> collect(Collector<? super T, A, R> collector);

  /**
   * An asynchronous equivalent to {@link Stream#min(java.util.Comparator)}.
   *
   * @param comparator comparator used to compare elements
   * @return an {@code Operation} representing the calculation of the minimum value
   */
  Operation<Optional<T>> min(Comparator<? super T> comparator);

  /**
   * An asynchronous equivalent to {@link Stream#max(java.util.Comparator)}.
   *
   * @param comparator comparator used to compare elements
   * @return an {@code Operation} representing the calculation of the maximum value
   */
  Operation<Optional<T>> max(Comparator<? super T> comparator);

  /**
   * An asynchronous equivalent to {@link Stream#count()}.
   *
   * @return an {@code Operation} representing the length of the stream
   */
  Operation<Long> count();

  /**
   * An asynchronous equivalent to {@link Stream#anyMatch(java.util.function.Predicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if at least one elements satisfies the predicate
   */
  Operation<Boolean> anyMatch(Predicate<? super T> predicate);

  /**
   * An asynchronous equivalent to {@link Stream#allMatch(java.util.function.Predicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if all the elements satisfy the predicate
   */
  Operation<Boolean> allMatch(Predicate<? super T> predicate);

  /**
   * An asynchronous equivalent to {@link Stream#noneMatch(java.util.function.Predicate)}.
   *
   * @param predicate predicate to apply to the elements in the stream
   * @return an {@code Operation} returning {@code true} if none of the elements satisfy the predicate
   */
  Operation<Boolean> noneMatch(Predicate<? super T> predicate);

  /**
   * An asynchronous equivalent to {@link Stream#findFirst()}.
   *
   * @return an {@code Operation} returning the first element of the stream
   */
  Operation<Optional<T>> findFirst();

  /**
   * An asynchronous equivalent to {@link Stream#findAny()}.
   *
   * @return an {@code Operation} returning an element of the stream
   */
  Operation<Optional<T>> findAny();
}
