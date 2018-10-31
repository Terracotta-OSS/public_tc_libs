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
import com.terracottatech.store.stream.RecordStream;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Stream;

import static java.util.Spliterator.CONCURRENT;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ShardedMutableRecordStreamTest {
  @Test
  public void testLimitIsDeferred() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.limit(anyLong())).then(RETURNS_SELF);
    when(b.limit(anyLong())).then(RETURNS_SELF);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>generate(() -> mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>generate(() -> mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedMutableRecordStream<String> stream = new ShardedMutableRecordStream<>(shards, 0, source);

    RecordStream<String> derived = stream.limit(10);

    assertThat(derived, instanceOf(DeferredMutableRecordStream.class));
    assertThat(derived.count(), is(10L));

    verify(a).limit(10L);
    verify(b).limit(10L);

    verify(a).spliterator();
    verify(b, never()).spliterator();
  }

  @Test
  public void testSkipIsDeferred() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedMutableRecordStream<String> stream = new ShardedMutableRecordStream<>(shards, 0, source);

    RecordStream<String> derived = stream.skip(1);

    assertThat(derived, instanceOf(DeferredMutableRecordStream.class));
    assertThat(derived.count(), is(1L));

    verify(a, never()).skip(anyLong());
    verify(b, never()).skip(anyLong());

    verify(a).spliterator();
    verify(b).spliterator();
  }

  @Test
  public void testMutateIsSharded() {
    MutableRecordStream<String> a = mock(MutableRecordStream.class);
    MutableRecordStream<String> b = mock(MutableRecordStream.class);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedMutableRecordStream<String> stream = new ShardedMutableRecordStream<>(shards, 0, source);

    UpdateOperation<String> transform = UpdateOperation.install();
    stream.mutate(transform);

    verify(a).mutate(transform);
    verify(b).mutate(transform);
  }

  @Test
  public void testDeleteIsSharded() {
    MutableRecordStream<String> a = mock(MutableRecordStream.class);
    MutableRecordStream<String> b = mock(MutableRecordStream.class);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedMutableRecordStream<String> stream = new ShardedMutableRecordStream<>(shards, 0, source);

    stream.delete();

    verify(a).delete();
    verify(b).delete();
  }

  @Test
  public void testMutateThenIsSharded() {
    MutableRecordStream<String> a = mock(MutableRecordStream.class);
    MutableRecordStream<String> b = mock(MutableRecordStream.class);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = new AbstractShardedRecordStreamTest.Impl(Collections.emptyList());

    ShardedMutableRecordStream<String> stream = new ShardedMutableRecordStream<>(shards, source.with(), source);

    UpdateOperation<String> transform = UpdateOperation.install();
    Stream<Tuple<Record<String>, Record<String>>> derived = stream.mutateThen(transform);

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.spliterator().characteristics(), is(DISTINCT | CONCURRENT | NONNULL));

    verify(a).mutateThen(transform);
    verify(b).mutateThen(transform);
  }

  @Test
  public void testDeleteThenIsSharded() {
    MutableRecordStream<String> a = mock(MutableRecordStream.class);
    MutableRecordStream<String> b = mock(MutableRecordStream.class);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = new AbstractShardedRecordStreamTest.Impl(Collections.emptyList());

    ShardedMutableRecordStream<String> stream = new ShardedMutableRecordStream<>(shards, source.with(), source);

    Stream<Record<String>> derived = stream.deleteThen();

    assertThat(derived, instanceOf(ShardedReferenceStream.class));
    assertThat(derived.spliterator().characteristics(), is(DISTINCT | CONCURRENT | NONNULL));

    verify(a).deleteThen();
    verify(b).deleteThen();
  }
}
