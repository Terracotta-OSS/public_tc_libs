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

package com.terracottatech.store.client.stream;

import com.terracottatech.store.StoreRetryableStreamTerminatedException;
import com.terracottatech.store.common.dataset.stream.WrappedDoubleStream;
import com.terracottatech.store.common.dataset.stream.WrappedIntStream;
import com.terracottatech.store.common.dataset.stream.WrappedLongStream;
import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;

import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class ResilientWrappedIntStream<K extends Comparable<K>> extends WrappedIntStream {

  RootRemoteRecordStream<K> rootStream;

  public ResilientWrappedIntStream(RootRemoteRecordStream<K> rootStream, final IntStream wrappedStream) {
    super(wrappedStream);
    this.rootStream = rootStream;
  }

  public ResilientWrappedIntStream(RootRemoteRecordStream<K> rootStream, final IntStream wrappedStream, final boolean isHead) {
    super(wrappedStream, isHead);
    this.rootStream = rootStream;
  }

  @Override
  protected <R> WrappedReferenceStream<R> wrapReferenceStream(Stream<R> stream) {
    return new ResilientWrappedReferenceStream<>(rootStream, stream, false);
  }

  @Override
  protected WrappedIntStream wrapIntStream(IntStream stream) {
    return new ResilientWrappedIntStream<>(rootStream, stream, false);
  }

  @Override
  protected WrappedLongStream wrapLongStream(LongStream stream) {
    return new ResilientWrappedLongStream<>(rootStream, stream, false);
  }

  @Override
  protected WrappedDoubleStream wrapDoubleStream(DoubleStream stream) {
    return new ResilientWrappedDoubleStream<>(rootStream, stream, false);
  }

  @Override
  public IntStream peek(IntConsumer action) {
    rootStream.setNonRetryable();
    return super.peek(action);
  }

  @Override
  public void forEach(IntConsumer action) {
    rootStream.setNonRetryable();
    super.forEach(action);
  }

  @Override
  public void forEachOrdered(IntConsumer action) {
    rootStream.setNonRetryable();
    super.forEachOrdered(action);
  }

  @Override
  protected <R> R selfClose(Function<IntStream, R> terminal) {
    return super.selfClose(s -> {
      try {
        return terminal.apply(s);
      } catch (StoreRetryableStreamTerminatedException e) {
        return terminal.apply((IntStream) rootStream.reconstructStream());
      }
    });
  }
}
