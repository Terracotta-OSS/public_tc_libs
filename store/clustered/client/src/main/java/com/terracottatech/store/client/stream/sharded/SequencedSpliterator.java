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

import com.terracottatech.store.util.Exceptions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

@SuppressWarnings("try")
class SequencedSpliterator<T, SPLIT extends Spliterator<T>> implements Spliterator<T>, AutoCloseable {

  protected final Spliterator<Supplier<? extends SPLIT>> spliterators;
  private final int characteristics;

  private final List<Spliterator<T>> opened = new ArrayList<>();
  private Spliterator<T> current;

  SequencedSpliterator(Stream<Supplier<? extends SPLIT>> spliterators, int characteristics) {
    this(spliterators.spliterator(), characteristics);
  }

  SequencedSpliterator(Spliterator<Supplier<? extends SPLIT>> spliterators, int characteristics) {
    if ((characteristics & (SORTED | ORDERED)) != 0) {
      throw new IllegalArgumentException("Sequenced spliterators cannot be sorted or ordered");
    }
    this.spliterators = spliterators;
    this.characteristics = characteristics & ~(SIZED | SUBSIZED);
  }

  @Override
  public boolean tryAdvance(Consumer<? super T> action) {
    if (current != null || spliterators.tryAdvance(s -> opened.add(current = s.get()))) {
      do {
        //We must not eagerly consume from the spliterator as the user might want to split it
        if (current.tryAdvance(action)) {
          return true;
        }
      } while (spliterators.tryAdvance(s -> opened.add(current = s.get())));
    }
    return false;
  }

  @Override
  public void forEachRemaining(Consumer<? super T> action) {
    if (current != null) {
      current.forEachRemaining(action);
    }
    Collection<Spliterator<T>> splits = StreamSupport.stream(spliterators, false).map(s -> {
      Spliterator<T> split = s.get();
      opened.add(split);
      return split;
    }).collect(toList());

    if (splits.size() > 1) {
      processAvailableBatches(splits, action);
      while (waitForNewElements(splits, action) && processAvailableBatches(splits, action));
    }
    splits.forEach(s -> s.forEachRemaining(action));
  }

  private static <T> boolean waitForNewElements(Collection<Spliterator<T>> spliterators, Consumer<? super T> action) {
    return spliterators.stream().anyMatch(s -> s.tryAdvance(action));
  }

  private static <T> boolean processAvailableBatches(Collection<Spliterator<T>> spliterators, Consumer<? super T> action) {
    boolean processed = false;
    while (!spliterators.stream().allMatch(s -> {
      Spliterator<T> available = s.trySplit();
      if (available == null) {
        return true;
      } else {
        available.forEachRemaining(action);
        return false;
      }
    })) {
      processed = true;
    }

    return processed;
  }

  @Override
  public Spliterator<T> trySplit() {
    Spliterator<Supplier<? extends SPLIT>> spliterator = spliterators.trySplit();
    if (spliterator == null) {
      return null;
    } else {
      return new SequencedSpliterator<>(spliterator, characteristics());
    }
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return characteristics;
  }

  @Override
  public void close() throws Exception {
    opened.stream().filter(AutoCloseable.class::isInstance).map(AutoCloseable.class::cast)
        .map(s -> (Callable<Void>) (() -> { s.close(); return null; }))
        .reduce(Exceptions::composeCallables)
        .orElse(() -> null).call();
  }

  static class FactoryOfObj<T> extends AggregatingSpliteratorFactory<T, Spliterator<T>> {

    FactoryOfObj(Supplier<Spliterator<T>> a, Supplier<Spliterator<T>> b, int characteristics, boolean parallel) {
      super(a, b, characteristics, parallel);
    }

    @Override
    Spliterator<T> get(Stream<Supplier<? extends Spliterator<T>>> supplierStream, int characteristics) {
      return new SequencedSpliterator<>(supplierStream, characteristics);
    }
  }
}
