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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparingInt;
import static java.util.Spliterator.ORDERED;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class AbstractShardedBaseStreamTest {

  @Test
  public void testOrderedWithoutOrdering() {
    try {
      new Impl<>(null, null, ORDERED);
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testOrderingWithoutOrdered() {
    try {
      new Impl<>(null, comparingInt(Object::hashCode), 0);
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testShardsStreamClosedOnClose() {
    Stream<Stream<Object>> shards = mock(Stream.class);
    AbstractShardedBaseStream<Object, Stream<Object>> impl = new Impl<>(shards, null, 0, mock(AbstractShardedRecordStream.class));

    impl.close();

    verify(shards).close();
  }

  @Test
  public void testShardsInStreamClosedOnClose() {
    RecordStream<String> stream = mock(RecordStream.class);

    AbstractShardedBaseStream<Record<String>, Stream<Record<String>>> impl = new AbstractShardedRecordStreamTest.Impl(singletonList(stream));

    impl.close();

    verify(stream).close();
  }

  @Test
  public void testShardsInStreamClosedEvenIfOneFailsOnClose() {
    RecordStream<String> bad = mock(RecordStream.class);
    RuntimeException failure = new RuntimeException();
    doThrow(failure).when(bad).close();
    RecordStream<String> good = mock(RecordStream.class);

    AbstractShardedBaseStream<Record<String>, Stream<Record<String>>> impl = new AbstractShardedRecordStreamTest.Impl(asList(bad, good));

    try {
      impl.close();
      fail("Expected Throwable");
    } catch (RuntimeException e) {
      assertThat(e, sameInstance(failure));
    }

    verify(bad).close();
    verify(good).close();
  }

  @Test
  public void testCloseDelegatesCorrectly() {
    Stream<Stream<String>> shards = mock(Stream.class);
    AbstractShardedBaseStream<String, Stream<String>> impl = new Impl<>(shards, null, 0, mock(AbstractShardedRecordStream.class));

    impl.close();
    verify(shards).close();
  }

  @Test
  public void testIsNotParallelIfNothingIsParallel() {
    Stream<Object> stream = mock(Stream.class);
    Stream<Stream<Object>> shards = Stream.of(stream);
    AbstractShardedBaseStream<Object, Stream<Object>> impl = new Impl<>(shards, null, 0, mock(AbstractShardedRecordStream.class));

    assertFalse(impl.isParallel());
  }

  @Test
  public void testIsParallelIfStreamOfShardsIsParallel() {
    Stream<Object> stream = mock(Stream.class);
    Stream<Stream<Object>> shards = Stream.of(stream).parallel();
    AbstractShardedBaseStream<Object, Stream<Object>> impl = new Impl<>(shards, null, 0, mock(AbstractShardedRecordStream.class));

    assertTrue(impl.isParallel());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testIsParallelIfASingleShardIsParallel() {
    RecordStream<String> streamA = mock(RecordStream.class);
    when(streamA.isParallel()).thenReturn(true);
    RecordStream<String> streamB = mock(RecordStream.class);

    Stream<RecordStream<String>> shards = Stream.of(streamA, streamB);

    AbstractShardedRecordStream source = mock(AbstractShardedRecordStream.class);
    when(source.getInitialStreams()).thenReturn(asList(streamA, streamB));

    AbstractShardedBaseStream<Object, Stream<Object>> impl = new Impl(shards, null, 0, source);

    assertTrue(impl.isParallel());
  }

  @Test
  public void testIsParallelIfEverythingIsParallel() {
    Stream<Object> streamA = mock(Stream.class);
    when(streamA.isParallel()).thenReturn(true);
    Stream<Object> streamB = mock(Stream.class);
    when(streamB.isParallel()).thenReturn(true);

    Stream<Stream<Object>> shards = Stream.of(streamA, streamB).parallel();

    AbstractShardedBaseStream<Object, Stream<Object>> impl = new Impl<>(shards, null, 0, mock(AbstractShardedRecordStream.class));

    assertTrue(impl.isParallel());
  }

  static class Impl<T> extends AbstractShardedBaseStream<T, Stream<T>> {

    protected Impl(List<? extends Stream<T>> shards, Comparator<? super T> ordering, int characteristics) {
      super(shards, ordering, characteristics);
    }

    protected Impl(Stream<Stream<T>> shards, Comparator<? super T> ordering, int characteristics, AbstractShardedRecordStream<?, ?> source) {
      super(shards, ordering, characteristics, source);
    }

    @Override
    public Iterator<T> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Spliterator<T> spliterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> sequential() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> parallel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> unordered() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
      throw new UnsupportedOperationException();
    }
  }
}
