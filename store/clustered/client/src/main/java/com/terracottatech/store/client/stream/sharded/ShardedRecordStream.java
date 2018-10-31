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
import com.terracottatech.store.stream.RecordStream;

import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Stream;

public class ShardedRecordStream<K extends Comparable<K>> extends AbstractShardedRecordStream<K, RecordStream<K>> {

  public ShardedRecordStream(Collection<RecordStream<K>> initial) {
    super(initial);
  }

  ShardedRecordStream(Stream<Stream<Record<K>>> shards, Comparator<? super Record<K>> ordering, int characteristics, AbstractShardedRecordStream<K, ?> source) {
    super(shards, ordering, characteristics, source);
  }

  @Override
  RecordStream<K> wrapAsRecordStream(Stream<Stream<Record<K>>> shards, Comparator<? super Record<K>> ordering, int characteristics) {
    return new ShardedRecordStream<>(shards, ordering, characteristics, getSource());
  }

  @Override
  public RecordStream<K> limit(long maxSize) {
    return new DetachedRecordStream<>(getInitialStreams(), rawLimit(maxSize));
  }

  @Override
  public RecordStream<K> skip(long n) {
    return new DetachedRecordStream<>(getInitialStreams(), rawSkip(n));
  }
}
