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

package com.terracottatech.store.logic;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class IntervalRecordPredicateTest {

  private static final IntCellDefinition def = CellDefinition.defineInt("def");

  @Test
  public void testUnionOfEmptyInput() {
    assertEquals(union(), Collections.emptyList());
  }

  @Test
  public void testUnionOfIdenticalIntervals() {
    Interval<Integer> interval = builder()
            .startOpen(0)
            .endClosed(5)
            .build();
    IntervalRecordPredicate<String, Integer> predicate1 = new IntervalRecordPredicate<>(def, interval);
    IntervalRecordPredicate<String, Integer> predicate2 = new IntervalRecordPredicate<>(def, interval);
    List<IntervalRecordPredicate<String, Integer>> union = union(predicate1, predicate2);
    assertEquals(Collections.singletonList(predicate1), union);
  }

  @Test
  public void testUnionOfIntersectingIntervals() {
    IntervalRecordPredicate<String, Integer> predicate1 = new IntervalRecordPredicate<>(def, builder()
            .startOpen(0)
            .endClosed(5)
            .build());
    IntervalRecordPredicate<String, Integer> predicate2 = new IntervalRecordPredicate<>(def, builder()
            .startOpen(2)
            .endClosed(10)
            .build());
    List<IntervalRecordPredicate<String, Integer>> union = union(predicate1, predicate2);
    IntervalRecordPredicate<String, Integer> expected = new IntervalRecordPredicate<>(def, builder()
            .startOpen(0)
            .endClosed(10)
            .build());
    assertEquals(Collections.singletonList(expected), union);
  }

  @Test
  public void testUnionOfDisjoinIntervals() {
    IntervalRecordPredicate<String, Integer> predicate1 = new IntervalRecordPredicate<>(def, builder()
            .startOpen(0)
            .endClosed(5)
            .build());
    IntervalRecordPredicate<String, Integer> predicate2 = new IntervalRecordPredicate<>(def, builder()
            .startOpen(6)
            .endClosed(10)
            .build());
    List<IntervalRecordPredicate<String, Integer>> union = union(predicate1, predicate2);
    assertEquals(Arrays.asList(predicate1, predicate2), union);
  }

  @SuppressWarnings("varargs")
  @SafeVarargs
  private final List<IntervalRecordPredicate<String, Integer>> union(IntervalRecordPredicate<String, Integer>... predicates) {
    return IntervalRecordPredicate.union(Arrays.asList(predicates))
            .collect(Collectors.toList());
  }

  private Interval.Builder<Integer> builder() {
    return Interval.builder();
  }
}
