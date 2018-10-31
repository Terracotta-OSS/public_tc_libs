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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


/**
 *
 * This class contains test cases for verifying correctness for different kinds of
 * predicates. As of now these tests asserts the correctness of various complex
 * predicates, and ensure this correctness even in the presence of secondary
 * indexes and possible index selection happening behind the scenes.
 *
 * Note: This test class does not assert whether the underlying index is used by the
 * {@code Spliterator}. Its sole purpose is to ensure correctness in the presences of
 * 1 or more indexes. There are other test classes such as
 * {@code RecordStreamOperationTest} and {@code RecordStream} that is asserting that the correct
 * index is selected.
 */
public class IndexUsageTest extends AbstractIndexUsageTest {

  @Test
  public void testCatalogIndexVisibility() throws Exception {
    SovereignDatasetImpl<String> sd = (SovereignDatasetImpl<String>) dataset;
    assertFalse(sd.hasSortedIndex(C3));
    Callable<SovereignIndex<Integer>> idx = dataset.getIndexing().createIndex(C3, SovereignIndexSettings.BTREE);
    assertFalse(sd.hasSortedIndex(C3));
    idx.call();
    assertTrue(sd.hasSortedIndex(C3));
  }

  @Test
  public void testMutationNotReseen() {
    final Predicate<Record<?>> pred1 = C1.value().isGreaterThan(3);
    long cnt = dataset.records().filter(pred1).count();
    assertThat(cnt, is(6l));
    AtomicInteger cnt2=new AtomicInteger(0);
    dataset.records().filter(pred1).forEach( r -> {
      System.out.println(r);
        if(cnt2.incrementAndGet()==1) {
          Integer c2 = r.get(C2).get();
          Integer c3 = r.get(C3).get();
          dataset.applyMutation(SovereignDataset.Durability.IMMEDIATE, "key_" + 5, r1 -> true, r2 -> {
            return Arrays.asList(new Cell<?>[] { C1.newCell(0), C2.newCell(c2), C3.newCell(c3) });
          });
        }
      });
    assertThat(cnt2.get(), is(5));
    System.out.println(cnt2.get());
  }

  @Test
  public void testSimpleLogicalConjunction() throws IOException, ExecutionException, InterruptedException {
    final Predicate<Record<?>> pred1 = C1.value().isGreaterThan(8).and(C2.value().isGreaterThan(18));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(1l));
    }
  }

  @Test
  public void testSimpleLogicalDisjunction() throws IOException, ExecutionException, InterruptedException {
    final Predicate<Record<?>> pred1 = C1.value().isGreaterThan(8).or(C2.value().isLessThan(11));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(2l));
    }
  }

  @Test
  public void testOverlappingGreaterThan() throws IOException, ExecutionException, InterruptedException {
    final Predicate<Record<?>> pred1 = C1.value().isGreaterThan(8).and(C1.value().isGreaterThan(5));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(1l));
    }

    final Predicate<Record<?>> pred2 = C1.value().isGreaterThan(5).and(C1.value().isGreaterThan(8));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count1 = recordStream.filter(pred2).count();
      assertThat(count1, is(1l));
    }
  }

  @Test
  public void testOverlappingGreaterThanWithMultiFilters() throws IOException, ExecutionException, InterruptedException {
    final Predicate<Record<?>> pred1 = C1.value().isGreaterThan(8);
    final Predicate<Record<?>> pred2 = C1.value().isGreaterThan(5);

    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).filter(pred2).count();
      assertThat(count, is(1l));
    }

    // now reverse the order
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count1 = recordStream.filter(pred2).filter(pred1).count();
      assertThat(count1, is(1l));
    }
  }

  @Test
  public void testLogicalAndPhysicalDuplicates() {
    Predicate<Record<?>> pred1 = C1.value().isGreaterThan(3).or(C1.value().isGreaterThan(3));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(6L));
    }
    pred1 = C1.value().isGreaterThan(3).or(C1.value().isLessThan(3));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(9L));
    }
    pred1 = C1.value().isGreaterThanOrEqualTo(3).or(C1.value().isGreaterThan(3));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(7L));
    }
    pred1 = C1.value().isGreaterThanOrEqualTo(8).or(C1.value().isLessThan(9));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(10L));
    }
    pred1 = C1.value().isGreaterThanOrEqualTo(2).or(C1.value().isGreaterThanOrEqualTo(2));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(8L));
    }
    pred1 = C1.value().isLessThan(3).or(C1.value().isLessThanOrEqualTo(2));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(3L));
    }
    pred1 = C1.value().isLessThanOrEqualTo(3).or(C1.value().isGreaterThan(8));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(5L));
    }
  }

  @Test
  public void testLogicalAndPhysicalDuplicatesEquals() {
    Predicate<Record<?>> pred1 = C1.value().is(3).or(C1.value().is(3));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred1).count();
      assertThat(count, is(1L));
    }

    Predicate<Record<?>> pred2 = pred1.or(C1.value().isLessThanOrEqualTo(3).and(C1.value().isGreaterThan(2)));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred2).count();
      assertThat(count, is(1L));
    }

    pred2 = pred1.or(C1.value().isGreaterThanOrEqualTo(3).and(C1.value().isLessThan(4))).or(pred1);
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred2).count();
      assertThat(count, is(1L));
    }

    pred2 = pred1.or(C1.value().isLessThan(5).and(C1.value().isGreaterThan(3)));
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      final long count = recordStream.filter(pred2).count();
      assertThat(count, is(2L));
    }
  }

  @Test
  public void testComplexPredicateForCorrectness() throws IOException, ExecutionException, InterruptedException {
    final Predicate<Record<?>> c1gt0 = C1.value().isGreaterThan(0);
    final Predicate<Record<?>> c1gt1 = C1.value().isGreaterThan(1);
    final Predicate<Record<?>> c1gte1 = C1.value().isGreaterThanOrEqualTo(1);
    final Predicate<Record<?>> c1lte1 = C1.value().isLessThanOrEqualTo(1);
    final Predicate<Record<?>> c1lt3 = C1.value().isLessThan(3);
    final Predicate<Record<?>> c1eq8 = C1.value().is(8);

    final Predicate<Record<?>> c2gt10 = C2.value().isGreaterThan(10);
    final Predicate<Record<?>> c2gt11 = C2.value().isGreaterThan(11);
    final Predicate<Record<?>> c2gte19 = C2.value().isGreaterThanOrEqualTo(19);
    final Predicate<Record<?>> c2lte12 = C2.value().isLessThanOrEqualTo(12);

    final Predicate<Record<?>> c3gte20 = C3.value().isGreaterThanOrEqualTo(20);
    final Predicate<Record<?>> c3lte100 = C3.value().isLessThanOrEqualTo(100);
    final Predicate<Record<?>> c3gte25 = C3.value().isGreaterThanOrEqualTo(25);
    final Predicate<Record<?>> c3lt1 = C3.value().isLessThan(1);
    final Predicate<Record<?>> c3eq25 = C3.value().is(25);

    try (final Stream<Record<String>> recordStream = dataset.records()) {
      // c1 has 0 to 9, so count of (c1 > 0 && c1 <= 1 && c1 > 1 && c1 < 3) should return 0
      final long count = recordStream.filter(c1gt0.and(c1lte1.and(c1gt1)).and(c1lt3)).count();
      assertThat(count, is(0l));
    }

    try (final Stream<Record<String>> recordStream = dataset.records()) {
      // c1 has 0 to 9, so count of (c1 > 0 && c1 <= 1 && c1 >= 1 && c1 < 3) should return 1
      final long count = recordStream.filter(c1gt0.and(c1lte1).and(c1gte1).and(c1lt3)).count();
      assertThat(count, is(1l));
    }

    try (final Stream<Record<String>> recordStream = dataset.records()) {
      // c1 has 0 to 9, c2 has 10 to 19, c3 has 20 to 29 so
      // count of (c1 > 0 && c2 > 10 && c3 > 20 && (c1 < 3 || c2 >= 19)) should return 3
      final long count = recordStream.filter(c2gt10.and(c1gt0).and(c3gte20).and(c1lt3.or(c2gte19))).count();
      assertThat(count, is(3l));
    }

    try (final Stream<Record<String>> recordStream = dataset.records()) {
      // c1 has 0 to 9, c2 has 10 to 19, c3 has 20 to 29 so
      // count of (c1 eq 8 || ((c2 > 11 && c2 <= 12) && (!(c3 >= 25) && (c3 <= 100)) is 2
      final Predicate<Record<?>> notOfC3gte25 = c3gte25.negate();
      final long count = recordStream.filter(c1eq8.or(c2gt11.and(c2lte12).and(notOfC3gte25.and(c3lte100)))).count();
      assertThat(count, is(2l));
    }

    try (final Stream<Record<String>> recordStream = dataset.records()) {
      // c1 has 0 to 9, c2 has 10 to 19, c3 has 20 to 29 so
      // count of (c3 = 25 && c2 > 10 && c1 >= 1 && !(c3 < 1)) is 1
      final Predicate<Record<?>> notOfC3lt1 = c3lt1.negate();
      final long count = recordStream.filter(c3eq25.and(c2gt10).and(c1gte1.and(notOfC3lt1))).count();
      assertThat(count, is(1l));
    }

    try (final Stream<Record<String>> recordStream = dataset.records()) {
      // c1 has 0 to 9, c2 has 10 to 19, c3 has 20 to 29 so
      // count of !(c1 = 8) && !(c2 >= 19) && !(c3 < 1) is 8
      final Predicate<Record<?>> notOfC1eq8 = c1eq8.negate();
      final Predicate<Record<?>> notOfC2gte19 = c2gte19.negate();
      final Predicate<Record<?>> notOfC3lt1 = c3lt1.negate();
      final long count = recordStream.filter(notOfC1eq8.and(notOfC2gte19).and(notOfC3lt1)).count();
      assertThat(count, is(8l));
    }
  }
}
