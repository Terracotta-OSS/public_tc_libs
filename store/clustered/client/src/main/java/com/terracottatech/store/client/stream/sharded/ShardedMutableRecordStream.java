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

import com.terracottatech.store.Record;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;

import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Stream;

import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SORTED;
import static java.util.function.Function.identity;

public class ShardedMutableRecordStream<K extends Comparable<K>> extends AbstractShardedRecordStream<K, MutableRecordStream<K>> implements MutableRecordStream<K> {

  public ShardedMutableRecordStream(Collection<MutableRecordStream<K>> rootStreams) {
    super(rootStreams);
  }

  ShardedMutableRecordStream(Stream<Stream<Record<K>>> shards, int characteristics, AbstractShardedRecordStream<K, ?> source) {
    super(shards, null, characteristics, source);
    if (is(ORDERED)) {
      throw new AssertionError("Mutable record streams should not be ordered");
    }
  }

  @Override
  MutableRecordStream<K> wrapAsRecordStream(Stream<Stream<Record<K>>> shards, Comparator<? super Record<K>> ordering, int characteristics) {
    return new ShardedMutableRecordStream<>(shards, characteristics, getSource());
  }

  @Override
  public MutableRecordStream<K> limit(long maxSize) {
    return deferring().limit(maxSize);
  }

  @Override
  public MutableRecordStream<K> skip(long n) {
    return deferring().skip(n);
  }

  @Override
  public void mutate(UpdateOperation<? super K> transform) {
    assert !is(ORDERED);
    consume(s -> ((MutableRecordStream<K>) s).mutate(transform));
  }

  @Override
  public Stream<Tuple<Record<K>, Record<K>>> mutateThen(UpdateOperation<? super K> transform) {
    assert !is(ORDERED);
    return derive(s -> ((MutableRecordStream<K>) s).mutateThen(transform), null, without(SORTED));
  }

  @Override
  public void delete() {
    assert !is(ORDERED);
    consume(s -> ((MutableRecordStream<K>) s).delete());
  }

  @Override
  public Stream<Record<K>> deleteThen() {
    assert !is(ORDERED);
    return derive(s -> ((MutableRecordStream<K>) s).deleteThen(), null, without(ORDERED));
  }

  private MutableRecordStream<K> deferring() {
    Stream<Stream<Record<K>>> shards = apply(identity());
    return new DeferredMutableRecordStream<>(
        new ShardedRecordStream<>(shards, getOrdering(), with(), getSource()),
        new ShardedMutableRecordStream<>(shards, with(), getSource()));
  }
}
