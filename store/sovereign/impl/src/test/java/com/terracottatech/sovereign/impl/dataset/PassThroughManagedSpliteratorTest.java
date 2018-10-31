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

package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.AnimalsDataset;
import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.store.Record;
import com.terracottatech.store.internal.InternalRecord;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.sovereign.impl.dataset.SpliteratorUtility.countSplits;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Clifford W. Johnson
 */
public class PassThroughManagedSpliteratorTest {

  private SovereignDataset<String> dataset;

  @Before
  public void setUp() throws Exception {
    this.dataset = AnimalsDataset.createDataset(16*1024 * 1024);
    AnimalsDataset.addIndexes(this.dataset);
  }

  @After
  public void tearDown() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
    this.dataset = null;
  }

  @SuppressWarnings("StatementWithEmptyBody")
  @Test
  public void testTryAdvanceAllPass() throws Exception {
    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(stream.spliterator(), () -> new FilteringManagedAction<>((r) -> true));

      final AtomicLong count = new AtomicLong(0);
      while (testSpliterator.tryAdvance(r -> count.incrementAndGet())) {
        // empty loop
      }
      assertThat(count.get(), is(this.dataset.records().count()));
    }
  }

  @SuppressWarnings("StatementWithEmptyBody")
  @Test
  public void testTryAdvanceAllDropped() throws Exception {
    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(stream.spliterator(), () -> new FilteringManagedAction<>((r) -> false));

      final AtomicLong count = new AtomicLong(0);
      while (testSpliterator.tryAdvance(r -> count.incrementAndGet())) {
        // empty loop
      }
      assertThat(count.get(), is(0L));
    }
  }

  @SuppressWarnings("StatementWithEmptyBody")
  @Test
  public void testTryAdvancedSomeDropped() throws Exception {
    @SuppressWarnings("unchecked")
    final Predicate<Record<String>> predicate =
        (Predicate<Record<String>>) (Object) TAXONOMIC_CLASS.value().is("mammal");

    final List<String> expectedAnimals = this.dataset.records().filter(predicate).map(Record::getKey).collect(toList());

    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(stream.spliterator(), () -> new FilteringManagedAction<>(predicate));

      final List<String> visitedKeys = new ArrayList<>();
      while (testSpliterator.tryAdvance(r -> visitedKeys.add(r.getKey()))) {
        // empty loop
      }
      assertThat(visitedKeys, containsInAnyOrder(expectedAnimals.toArray(new String[expectedAnimals.size()])));
    }
  }

  @Test
  public void testTrySplit() throws Exception {
    try (final Stream<Record<String>> testStream = this.dataset.records();
         final Stream<Record<String>> expectedStream = this.dataset.records()) {
      final Spliterator<Record<String>> expectedSpliterator = expectedStream.spliterator();
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(testStream.spliterator(), () -> new FilteringManagedAction<>((r) -> true));

      assertThat(countSplits(testSpliterator), is(countSplits(expectedSpliterator)));
    }
  }

  @Test
  public void testForEachRemaining() throws Exception {
    final List<String> expectedKeys = this.dataset.records().map(Record::getKey).collect(toList());

    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(stream.spliterator(), () -> new FilteringManagedAction<>((r) -> true));

      final List<String> visitedKeys = new ArrayList<>();
      testSpliterator.forEachRemaining(r -> visitedKeys.add(r.getKey()));
      assertThat(visitedKeys, containsInAnyOrder(expectedKeys.toArray(new String[expectedKeys.size()])));
    }
  }

  @Test
  public void testCharacteristics() throws Exception {
    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> sourceSpliterator = stream.spliterator();
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(sourceSpliterator, () -> new FilteringManagedAction<>((r) -> true));
      assertThat(testSpliterator.characteristics(), is(sourceSpliterator.characteristics()));
    }
  }

  @Test
  public void testEstimateSize() throws Exception {
    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> sourceSpliterator = stream.spliterator();
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(sourceSpliterator, () -> new FilteringManagedAction<>((r) -> true));
      assertThat(testSpliterator.estimateSize(), is(sourceSpliterator.estimateSize()));
    }
  }

  @Test
  public void testGetComparator() throws Exception {
    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> sourceSpliterator = stream.spliterator();
      final Spliterator<Record<String>> testSpliterator =
          new PassThroughManagedSpliterator<>(sourceSpliterator, () -> new FilteringManagedAction<>((r) -> true));

      Comparator<? super Record<String>> expectedComparator;
      try {
        expectedComparator = sourceSpliterator.getComparator();
        try {
          final Comparator<? super Record<String>> actualComparator = testSpliterator.getComparator();
          assertThat(actualComparator, is(equalTo(expectedComparator)));
        } catch (IllegalStateException e) {
          fail("Unexpected IllegalStateException");
        }
      } catch (IllegalStateException e) {
        /* The spliterator source isn't SORTED. */
        try {
          testSpliterator.getComparator();
          fail();
        } catch (IllegalStateException expected) {
          // Expected
        }
      }
    }
  }

  @Test
  public void testClose() throws Exception {
    final FilteringManagedAction<String> managedAction = spy(new FilteringManagedAction<>((r) -> true));

    try (final Stream<Record<String>> stream = this.dataset.records()) {
      final Spliterator<Record<String>> sourceSpliterator = stream.spliterator();
      final PassThroughManagedSpliterator<String> testSpliterator =
          new PassThroughManagedSpliterator<>(sourceSpliterator, () -> managedAction);

      testSpliterator.close();
      verify(managedAction).close();
    }
  }

  /**
   * A {@code ManagedAction} implementation that opens only those {@code Record}s that pass a
   * {@code Predicate}.
   *
   * @param <K> the {@code ManagedAction} key type
   */
  private static class FilteringManagedAction<K extends Comparable<K>>
      implements ManagedAction<K>, AutoCloseable {

    private final Predicate<Record<K>> filter;
    private volatile InternalRecord<K> current;

    public FilteringManagedAction(final Predicate<Record<K>> filter) {
      Objects.requireNonNull(filter, "filter");
      this.filter = filter;
    }

    @Override
    public InternalRecord<K> begin(final InternalRecord<K> record) {
      this.current = filter.test(record) ? record : null;
      return this.current;
    }

    @Override
    public void end(final InternalRecord<K> record) {
      if (this.current != record) {
        throw new AssertionError(String.format("Expecting record key='%s', found key='%s'",
            (this.current == null ? "null": this.current.getKey()),
            (record == null ? "null" : record.getKey())));
      }
    }

    @Override
    public SovereignContainer<K> getContainer() {
      return null;
    }

    @Override
    public void close() {
    }
  }
}
