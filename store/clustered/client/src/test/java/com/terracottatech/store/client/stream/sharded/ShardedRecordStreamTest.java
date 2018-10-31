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
import org.junit.Test;

import java.util.stream.Stream;

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
public class ShardedRecordStreamTest {

  @SuppressWarnings("rawtypes")
  @Test
  public void testLimitShardsMergesAndDetaches() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.limit(anyLong())).then(RETURNS_SELF);
    when(b.limit(anyLong())).then(RETURNS_SELF);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>generate(() -> mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>generate(() -> mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedRecordStream<String> stream = new ShardedRecordStream(shards, null, 0, source);

    RecordStream<String> derived = stream.limit(10);

    assertThat(derived, instanceOf(DetachedRecordStream.class));
    assertThat(derived.count(), is(10L));

    verify(a).limit(10L);
    verify(b).limit(10L);

    try {
      verify(a).spliterator();
      verify(b, never()).spliterator();
    } catch (Throwable e) {
      try {
        verify(a, never()).spliterator();
        verify(b).spliterator();
      } catch (Throwable f) {
        f.addSuppressed(e);
        throw f;
      }
    }
  }

  @Test
  public void testSkipMergesAndDetaches() {
    RecordStream<String> a = mock(RecordStream.class);
    RecordStream<String> b = mock(RecordStream.class);
    when(a.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());
    when(b.spliterator()).thenReturn(Stream.<Record<String>>of(mock(Record.class)).spliterator());

    Stream<Stream<Record<String>>> shards = Stream.of(a, b);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.registerCommonStream(any())).then(returnsFirstArg());

    ShardedRecordStream<String> stream = new ShardedRecordStream(shards, null, 0, source);

    RecordStream<String> derived = stream.skip(1);

    assertThat(derived, instanceOf(DetachedRecordStream.class));
    assertThat(derived.count(), is(1L));

    verify(a, never()).skip(anyLong());
    verify(b, never()).skip(anyLong());

    verify(a).spliterator();
    verify(b).spliterator();
  }
}
