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
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SIZED;
import static java.util.Spliterator.SORTED;
import static java.util.Spliterator.SUBSIZED;
import static java.util.Spliterators.spliterator;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class SequencedSpliteratorTest {

  @Test
  public void testThrowsOnSorted() {
    try {
      new SequencedSpliterator<>(Stream.empty(), SORTED);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testThrowsOnOrdered() {
    try {
      new SequencedSpliterator<>(Stream.empty(), ORDERED);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testClearsSizedAndSubsized() {
    SequencedSpliterator<Object, Spliterator<Object>> spliterator = new SequencedSpliterator<>(Stream.empty(), ~(ORDERED | SORTED));

    assertThat(spliterator.characteristics(), is(~(ORDERED | SORTED | SIZED | SUBSIZED)));
  }

  @Test
  public void testSizeIsUnknown() {
    SequencedSpliterator<Object, Spliterator<Object>> spliterator = new SequencedSpliterator<>(Stream.empty(), 0);

    assertThat(spliterator.estimateSize(), is(Long.MAX_VALUE));
  }

  @Test
  public void testUnopenedCloseableSpliteratorsAreNotClosed() throws Exception {
    @SuppressWarnings("unchecked")
    Spliterator<Object> closeableSpliterator = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));

    SequencedSpliterator<Object, Spliterator<Object>> spliterator = new SequencedSpliterator<>(Stream.of(
        () -> closeableSpliterator), 0);

    spliterator.close();

    verify((AutoCloseable) closeableSpliterator, never()).close();
  }

  @Test
  public void testOpenedCloseableSpliteratorsAreNotClosed() throws Exception {
    @SuppressWarnings("unchecked")
    Spliterator<Object> closeableSpliterator = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));

    SequencedSpliterator<Object, Spliterator<Object>> spliterator = new SequencedSpliterator<>(Stream.of(
        () -> closeableSpliterator), 0);

    spliterator.tryAdvance(null);

    spliterator.close();

    verify((AutoCloseable) closeableSpliterator).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCloseableSpliteratorExceptionsAreCollectedCorrectly() throws Exception {
    Spliterator<Object> failingCloseableSpliterator1 = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));
    Exception throwable1 = new Exception();
    doThrow(throwable1).when((AutoCloseable) failingCloseableSpliterator1).close();
    Spliterator<Object> failingCloseableSpliterator2 = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));
    Exception throwable2 = new Exception();
    doThrow(throwable2).when((AutoCloseable) failingCloseableSpliterator2).close();

    Spliterator<Object> closeableSpliterator = mock(Spliterator.class, withSettings().extraInterfaces(AutoCloseable.class));

    SequencedSpliterator<Object, Spliterator<Object>> spliterator = new SequencedSpliterator<>(Stream.of(
        () -> failingCloseableSpliterator1,
        () -> failingCloseableSpliterator2,
        () -> closeableSpliterator), 0);

    spliterator.forEachRemaining(null);

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
  public void testTryAdvanceLazilyOpens() {

    Spliterator<String> spliterator = new SequencedSpliterator<>(Stream.of(
        () -> spliterator(asList("bar", "foo"), 0),
        () -> { throw new AssertionError(); }), 0);

    List<String> strings = new ArrayList<>();
    spliterator.tryAdvance(strings::add);
    spliterator.tryAdvance(strings::add);

    assertThat(strings, containsInAnyOrder("bar", "foo"));
  }

  @Test
  public void testTryAdvanceReturnsEverything() {
    Spliterator<String> spliterator = new SequencedSpliterator<>(Stream.of(
        () -> spliterator(asList("bar", "foo"), 0),
        () -> spliterator(asList("baz", "egg"), 0)), 0);

    List<String> strings = new ArrayList<>();
    while (spliterator.tryAdvance(strings::add));

    assertThat(strings, containsInAnyOrder("bar", "baz", "egg", "foo"));
  }

  @Test
  public void testForEachRemainingReturnsEverything() {
    Spliterator<String> spliterator = new SequencedSpliterator<>(Stream.of(
        () -> spliterator(asList("bar", "foo"), 0),
        () -> spliterator(asList("baz", "egg"), 0)), 0);

    List<String> strings = new ArrayList<>();
    spliterator.forEachRemaining(strings::add);

    assertThat(strings, containsInAnyOrder("bar", "baz", "egg", "foo"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testForEachRemainingAccessesFirstAvailable() {
    Spliterator<String> a = mock(Spliterator.class);
    Spliterator<String> b = mock(Spliterator.class);
    when(a.trySplit())
        .thenReturn(null)
        .thenReturn(spliterator(asList("bar", "foo"), 0))
        .thenReturn(null);
    when(b.trySplit())
        .thenReturn(spliterator(asList("baz", "egg"), 0))
        .thenReturn(null);

    Spliterator<String> spliterator = new SequencedSpliterator<>(Stream.of(() -> a, () -> b), 0);

    List<String> strings = new ArrayList<>();

    spliterator.forEachRemaining(strings::add);

    assertThat(strings, contains("baz", "egg", "bar", "foo"));
  }

  @Test
  public void testForEachRemainingCallsRemainingWithSingleRemoteSpliterator() {
    @SuppressWarnings("unchecked")
    Spliterator<String> split = mock(Spliterator.class);

    Spliterator<String> spliterator = new SequencedSpliterator<>(Stream.of(() -> split), 0);

    spliterator.forEachRemaining(a -> {});

    verify(split).forEachRemaining(any());
  }

  @Test
  public void testTrySplitSplitsByShards() {
    Spliterator<String> spliterator1 = new SequencedSpliterator<>(Stream.of(
        () -> spliterator(asList("bar", "foo"), 0),
        () -> spliterator(asList("baz", "egg"), 0)), 0);

    Spliterator<String> spliterator2 = spliterator1.trySplit();

    List<String> strings1 = new ArrayList<>();
    spliterator1.forEachRemaining(strings1::add);
    assertThat(strings1, containsInAnyOrder("baz", "egg"));

    List<String> strings2 = new ArrayList<>();
    spliterator2.forEachRemaining(strings2::add);
    assertThat(strings2, containsInAnyOrder("bar", "foo"));
  }
}
