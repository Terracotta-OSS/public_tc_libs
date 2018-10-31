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

package com.terracottatech.store.client.stream.sharded;

import com.terracottatech.store.common.dataset.stream.WrappedDoubleStream;
import com.terracottatech.store.common.dataset.stream.WrappedIntStream;
import com.terracottatech.store.common.dataset.stream.WrappedLongStream;
import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;

import java.lang.reflect.Proxy;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.terracottatech.store.client.stream.AbstractRemoteBaseStream.STREAM_USED_OR_CLOSED;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;

abstract class AbstractShardedBaseStream<T, STREAM extends BaseStream<T, STREAM>> implements BaseStream<T, STREAM> {

  private final Stream<STREAM> shards;
  private final int characteristics;
  private final Comparator<? super T> ordering;
  private final AbstractShardedRecordStream<?, ?> source;

  private volatile boolean closed = false;

  AbstractShardedBaseStream(Collection<? extends STREAM> shards, Comparator<? super T> ordering, int characteristics) {
    if (((characteristics & ORDERED) == ORDERED) ^ (ordering != null)) {
      throw new IllegalArgumentException();
    }
    Stream<STREAM> initialStream = shards.stream().map(s -> (STREAM) s);
    /*
     * Close the shards when this stream is closed.
     */
    for (STREAM s : shards) {
      initialStream = initialStream.onClose(s::close);
    }
    this.shards = initialStream;
    this.ordering = ordering;
    this.characteristics = characteristics;
    this.source = (AbstractShardedRecordStream<?, ?>) this;
  }

  AbstractShardedBaseStream(Stream<STREAM> shards, Comparator<? super T> ordering, int characteristics, AbstractShardedRecordStream<?, ?> source) {
    if (((characteristics & ORDERED) == ORDERED) ^ (ordering != null)) {
      throw new IllegalArgumentException();
    }
    this.shards = shards;
    this.ordering = ordering;
    this.characteristics = characteristics;
    this.source = requireNonNull(source);
  }

  AbstractShardedRecordStream<?,?> getSource() {
    return source;
  }

  <U> Stream<U> apply(Function<STREAM, U> operation) {
    return shards.map(operation);
  }

  IntStream applyToInt(ToIntFunction<STREAM> operation) {
    return shards.mapToInt(operation);
  }

  LongStream applyToLong(ToLongFunction<STREAM> operation) {
    return shards.mapToLong(operation);
  }

  DoubleStream applyToDouble(ToDoubleFunction<STREAM> operation) {
    return shards.mapToDouble(operation);
  }

  void consume(Consumer<STREAM> operation) {
    shards.forEach(operation);
  }

  int with(int... add) {
    int result = characteristics;
    for (int a : add) {
      result |= a;
    }
    return result;
  }

  int without(int... remove) {
    int result = characteristics;
    for (int r : remove) {
      result &= ~r;
    }
    return result;
  }

  boolean is(int... needed) {
    boolean is = true;
    for (int c : needed) {
      is &= ((characteristics & c) == c);
    }
    return is;
  }

  Comparator<? super T> getOrdering() {
    return ordering;
  }

  @Override
  public boolean isParallel() {
    return shards.isParallel() || getSource().getInitialStreams().stream().anyMatch(s -> s.isParallel());
  }

  @Override
  public STREAM onClose(Runnable closeHandler) {
    if (closed) {
      throw new IllegalStateException(STREAM_USED_OR_CLOSED);
    } else {
      Stream<STREAM> newShards = shards.onClose(closeHandler);
      assert newShards == shards;
      @SuppressWarnings("unchecked")
      STREAM stream = (STREAM) this;
      return stream;
    }
  }

  @Override
  public void close() {
    try {
      if (!closed) {
        //Trigger pipeline formation if it hasn't been already (for explain purposes).
        //This must be complex enough so that the JDK cannot optimize it out.
        //Count has been eliminated as a candidate since sized spliterators short-circuit it.
        shards.forEach(s -> {});
      }
    } catch (IllegalStateException e) {
      //ignore - stream was likely consumed already
    } finally {
      closed = true;
      shards.close();
    }
  }

  <U> Stream<U> terminateWith(Function<STREAM, U> operation) {
    return linkClosureAndMerge(stream -> new WrappedReferenceStream<>(stream.apply(operation)));
  }

  IntStream terminateWithInt(ToIntFunction<STREAM> operation) {
    return linkClosureAndMerge(stream -> new WrappedIntStream(stream.applyToInt(operation)));
  }

  LongStream terminateWithLong(ToLongFunction<STREAM> operation) {
    return linkClosureAndMerge(stream -> new WrappedLongStream(stream.applyToLong(operation)));
  }

  DoubleStream terminateWithDouble(ToDoubleFunction<STREAM> operation) {
    return linkClosureAndMerge(stream -> new WrappedDoubleStream(stream.applyToDouble(operation)));
  }

  <U, W extends BaseStream<U, W>> W linkClosureAndMerge(Function<AbstractShardedBaseStream<T, STREAM>, W> merger) {
    /*
     * We can't call onClose after the stream is terminated so we must put a close handler in place *before*
     * we have a reference to the merged stream that must be closed.  This then opens up a race where the shard streams
     * might be closed before we finished executing this method. To get around *that* we must use a CompletableFuture to
     * ensure that the onClose handler will wait for the merged stream to be linked up before we finish.
     */
    CompletableFuture<BaseStream<?, ?>> pendingMerge = new CompletableFuture<>();

    AbstractShardedBaseStream<T, STREAM> closeLinked = (AbstractShardedBaseStream<T, STREAM>) onClose(
        () -> pendingMerge.join().close()
    );

    try {
      W merged = merger.apply(closeLinked);
      pendingMerge.complete(merged);
      return getSource().registerCommonStream(merged.onClose(closeLinked::close));
    } catch (Throwable t) {
      pendingMerge.completeExceptionally(t);
      throw t;
    }
  }

  <F> F explainedAs(String explain, F function) {
    if (getSource().requiresExplanation()) {
      Class<?> type = function.getClass();
      @SuppressWarnings("unchecked")
      F proxyInstance = (F) Proxy.newProxyInstance(type.getClassLoader(), type.getInterfaces(), (proxy, method, args) -> {
        if ("toString".equals(method.getName()) && method.getParameterCount() == 0) {
          try {
            return MessageFormat.format(explain, function);
          } catch (IllegalArgumentException e) {
            return "Illegal Message Format: " + explain + " [" + function + "]";
          }
        } else {
          return method.invoke(function, args);
        }
      });
      return proxyInstance;
    } else {
      return function;
    }
  }
}
