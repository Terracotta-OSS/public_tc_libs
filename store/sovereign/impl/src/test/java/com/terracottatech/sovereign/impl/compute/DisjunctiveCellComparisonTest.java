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

package com.terracottatech.sovereign.impl.compute;

import com.terracottatech.store.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Tests {@link DisjunctiveCellComparison}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DisjunctiveCellComparisonTest {

  @Mock
  private IndexedCellRangePredicate<Integer, ?> range;

  @Mock
  private IndexedCellRangePredicate<Integer, ?> redundantRange;

  @Mock
  private IndexedCellRangePredicate<Integer, ?> alternativeRange;

  @Mock
  private Record<Integer> match;

  @Mock
  private Record<Integer> mismatch;

  @Before
  public void setUp() {
    when(range.test(match)).thenReturn(true);
    when(range.test(mismatch)).thenReturn(false);

    when(redundantRange.test(match)).thenReturn(true);
    when(redundantRange.test(mismatch)).thenReturn(false);

    when(alternativeRange.test(match)).thenReturn(false);
    when(alternativeRange.test(mismatch)).thenReturn(true);
  }

  @Test
  public void testSingleRange() {
    List<IndexedCellRangePredicate<Integer, ?>> fragments = Collections.singletonList(range);
    DisjunctiveCellComparison<Integer> comparison = new DisjunctiveCellComparison<>(fragments);
    assertEquals(fragments.size(), comparison.countIndexRanges());

    IndexedCellRangePredicate<Integer, ?> first = comparison.indexRangeIterator().next();
    assertTrue(first.test(match));
    assertFalse(first.overlaps(match));
    assertFalse(first.test(mismatch));
    assertFalse(first.overlaps(mismatch));
  }

  /**
   * Test that only second occurrence of the duplicate range returns true.
   */
  @Test
  public void testPhysicalDuplicates() {
    List<IndexedCellRangePredicate<Integer, ?>> fragments = Arrays.asList(range, range);
    DisjunctiveCellComparison<Integer> comparison = new DisjunctiveCellComparison<>(fragments);
    assertEquals(fragments.size(), comparison.countIndexRanges());
    Iterator<IndexedCellRangePredicate<Integer, ?>> iterator = comparison.indexRangeIterator();

    IndexedCellRangePredicate<Integer, ?> first = iterator.next();
    assertTrue(first.test(match));
    assertTrue(first.overlaps(match));
    assertFalse(first.test(mismatch));
    assertFalse(first.overlaps(mismatch));

    IndexedCellRangePredicate<Integer, ?> duplicate = iterator.next();
    assertTrue(duplicate.test(match));
    assertFalse(duplicate.overlaps(match));
    assertFalse(duplicate.test(mismatch));
    assertFalse(duplicate.overlaps(mismatch));
  }


  /**
   * Test that only second occurrence of the logically redundant range returns true.
   */
  @Test
  public void testLogicalDuplicates() {
    List<IndexedCellRangePredicate<Integer, ?>> fragments = Arrays.asList(range, redundantRange);
    DisjunctiveCellComparison<Integer> comparison = new DisjunctiveCellComparison<>(fragments);
    assertEquals(fragments.size(), comparison.countIndexRanges());
    Iterator<IndexedCellRangePredicate<Integer, ?>> iterator = comparison.indexRangeIterator();

    IndexedCellRangePredicate<Integer, ?> first = iterator.next();
    assertTrue(first.test(match));
    assertTrue(first.overlaps(match));
    assertFalse(first.test(mismatch));
    assertFalse(first.overlaps(mismatch));

    IndexedCellRangePredicate<Integer, ?> redundant = iterator.next();
    assertTrue(redundant.test(match));
    assertFalse(redundant.overlaps(match));
    assertFalse(redundant.test(mismatch));
    assertFalse(redundant.overlaps(mismatch));
  }

  /**
   * Test that non overlapping ranges produce independent results.
   */
  @Test
  public void testAlternatives() {
    List<IndexedCellRangePredicate<Integer, ?>> fragments = Arrays.asList(range, alternativeRange);
    DisjunctiveCellComparison<Integer> comparison = new DisjunctiveCellComparison<>(fragments);
    assertEquals(fragments.size(), comparison.countIndexRanges());
    Iterator<IndexedCellRangePredicate<Integer, ?>> iterator = comparison.indexRangeIterator();

    IndexedCellRangePredicate<Integer, ?> first = iterator.next();
    assertTrue(first.test(match));
    assertFalse(first.overlaps(match));
    assertFalse(first.test(mismatch));
    assertTrue(first.overlaps(mismatch));

    IndexedCellRangePredicate<Integer, ?> alternative = iterator.next();
    assertFalse(alternative.test(match));
    assertFalse(alternative.overlaps(match));
    assertTrue(alternative.test(mismatch));
    assertFalse(alternative.overlaps(mismatch));
  }

  public static void main(String[] args) {
    DisjunctiveCellComparisonTest suite = new DisjunctiveCellComparisonTest();
    initMocks(suite);
    suite.setUp();
    run(suite, 1e3);

    Instant start = Instant.now();
    run(suite, 1e5);
    Duration duration = Duration.between(start, Instant.now());
    System.out.println(duration.toMillis());
  }

  private static void run(DisjunctiveCellComparisonTest suite, double times) {
    for (int i = 0; i < times; i++) {
      suite.testSingleRange();
      suite.testPhysicalDuplicates();
      suite.testLogicalDuplicates();
      suite.testAlternatives();
    }
  }
}
