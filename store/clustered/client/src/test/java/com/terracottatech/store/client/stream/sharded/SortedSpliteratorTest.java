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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Comparator.naturalOrder;
import static java.util.Spliterator.CONCURRENT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterators.emptySpliterator;
import static java.util.Spliterators.spliterator;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class SortedSpliteratorTest {

  @Test
  public void testNullOrderingFails() {
    try {
      new SortedSpliterator<>(Stream.empty(), null, 0);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public void testCharacteristicsMergePositively() {
    @SuppressWarnings("unchecked")
    Spliterator<String> everything = mock(Spliterator.class);
    when(everything.characteristics()).thenReturn(~0);

    Stream<Supplier<Spliterator<String>>> spliterators = Stream.of(() -> everything, () -> everything);

    assertThat(new SortedSpliterator<>(spliterators, naturalOrder(), ~0).characteristics(), is(~0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCharacteristicsMergeNegatively() {
    Spliterator<String> everything = mock(Spliterator.class);
    when(everything.characteristics()).thenReturn(~CONCURRENT); //concurrent does not merge in simple manner
    Spliterator<String> nothing = mock(Spliterator.class);
    when(nothing.characteristics()).thenReturn(0);

    Stream<Supplier<Spliterator<String>>> spliterators = Stream.of(() -> everything, () -> nothing);

    assertThat(new SortedSpliterator<>(spliterators, naturalOrder(), ~CONCURRENT).characteristics(), is(0));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMixedImmutableAndConcurrentSpliteratorsAreConcurrent() {
    Spliterator<String> everything = mock(Spliterator.class);
    when(everything.characteristics()).thenReturn(IMMUTABLE);
    Spliterator<String> nothing = mock(Spliterator.class);
    when(nothing.characteristics()).thenReturn(CONCURRENT);

    Stream<Supplier<Spliterator<String>>> spliterators = Stream.of(() -> everything, () -> nothing);

    assertThat(new SortedSpliterator<>(spliterators, naturalOrder(), 0).characteristics() & CONCURRENT, is(CONCURRENT));
  }

  @Test
  public void testTrySplitReturnsNull() {
    @SuppressWarnings("unchecked")
    Spliterator<String> splittable = mock(Spliterator.class);
    when(splittable.trySplit()).then(RETURNS_SELF);

    Stream<Supplier<Spliterator<String>>> spliterators = Stream.of(() -> splittable);

    SortedSpliterator<String> spliterator = new SortedSpliterator<>(spliterators, naturalOrder(), 0);

    assertThat(spliterator.trySplit(), nullValue());
  }

  @Test
  public void testEmptySpliteratorIsZeroSize() {
    SortedSpliterator<String> spliterator = new SortedSpliterator<>(Stream.empty(), naturalOrder(), 0);

    assertThat(spliterator.estimateSize(), is(0L));
  }

  @Test
  public void testSpliteratorSizesSum() {
    Stream<Supplier<? extends Spliterator<String>>> spliterators = Stream.of(
        () -> spliterator(asList("foo", "foo"), 0),
        () -> spliterator(asList("bar", "bar"), 0));
    SortedSpliterator<String> spliterator = new SortedSpliterator<>(spliterators, naturalOrder(), 0);

    assertThat(spliterator.estimateSize(), is(4L));
  }

  @Test
  public void testSingleUnsizedSpliteratorTriggersUnsized() {
    Stream<Supplier<? extends Spliterator<String>>> spliterators = Stream.of(
        () -> spliteratorUnknownSize(asList("foo", "foo").iterator(), 0),
        () -> spliterator(asList("bar", "bar"), 0));
    SortedSpliterator<String> spliterator = new SortedSpliterator<>(spliterators, naturalOrder(), 0);

    assertThat(spliterator.estimateSize(), is(Long.MAX_VALUE));
  }

  @Test
  public void testCloseableSpliteratorsAreClosed() throws Exception {
    @SuppressWarnings("unchecked")
    Spliterator<String> closeableSpliterator = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));

    SortedSpliterator<String> spliterator = new SortedSpliterator<>(Stream.of(() -> closeableSpliterator), naturalOrder(), 0);

    spliterator.close();

    verify((AutoCloseable) closeableSpliterator).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCloseableSpliteratorExceptionsAreCollectedCorrectly() throws Exception {
    Spliterator<String> failingCloseableSpliterator1 = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));
    Exception throwable1 = new Exception();
    doThrow(throwable1).when((AutoCloseable) failingCloseableSpliterator1).close();
    Spliterator<String> failingCloseableSpliterator2 = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));
    Exception throwable2 = new Exception();
    doThrow(throwable2).when((AutoCloseable) failingCloseableSpliterator2).close();

    Spliterator<String> closeableSpliterator = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));

    SortedSpliterator<String> spliterator = new SortedSpliterator<>(Stream.of(
        () -> failingCloseableSpliterator1,
        () -> failingCloseableSpliterator2,
        () -> closeableSpliterator), naturalOrder(), 0);

    try {
      spliterator.close();
      fail("Expected Throwable");
    } catch (Throwable t) {
      assertThat(t, sameInstance(throwable1));
      assertThat(t.getSuppressed(), arrayContaining(sameInstance(throwable2)));
    }

    verify((AutoCloseable) failingCloseableSpliterator1).close();
    verify((AutoCloseable) failingCloseableSpliterator2).close();
    verify((AutoCloseable) closeableSpliterator).close();
  }

  @Test
  public void testEmptySpliteratorCannotAdvance() {
    SortedSpliterator<String> spliterator = new SortedSpliterator<>(Stream.empty(), naturalOrder(), 0);

    assertFalse(spliterator.tryAdvance(s -> {
      throw new AssertionError(s);
    }));
  }

  @Test
  public void testSpliteratorOfEmptySpliteratorCannotAdvance() {
    SortedSpliterator<String> spliterator = new SortedSpliterator<>(Stream.of(() -> emptySpliterator()), naturalOrder(), 0);

    assertFalse(spliterator.tryAdvance(s -> {
      throw new AssertionError(s);
    }));
  }

  @Test
  public void testSpliteratorPreservesOrder() {
    SortedSpliterator<String> spliterator = new SortedSpliterator<>(Stream.of(() -> spliterator(asList("bar", "foo"), 0), () -> spliterator(asList("baz", "egg"), 0)), naturalOrder(), 0);

    List<String> strings = new ArrayList<>();
    while (spliterator.tryAdvance(strings::add));

    assertThat(strings, contains("bar", "baz", "egg", "foo"));
  }
}
