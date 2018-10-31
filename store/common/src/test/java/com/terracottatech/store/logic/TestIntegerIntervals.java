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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.terracottatech.store.logic.Combinatorics.allOrderings;
import static junit.framework.TestCase.assertEquals;

/**
 * Test integer intervals
 */
public class TestIntegerIntervals extends TestIntervals<Integer> {

  private final Interval<Integer> CLOSED_START = builder().startClosed(-1).build();
  private final Interval<Integer> CLOSED_END = builder().endClosed(1).build();
  private final Interval<Integer> CLOSED = builder().startClosed(-1).endClosed(1).build();
  private final Interval<Integer> OPEN_TO_CLOSED = builder().startClosed(-1).endOpen(1).build();
  private final Interval<Integer> CLOSED_TO_OPEN = builder().startOpen(-1).endClosed(1).build();
  private final Interval<Integer> OPEN = builder().startOpen(-1).endOpen(1).build();
  private final Interval<Integer> OPEN_DEGENERATE = builder().startOpen(0).endOpen(0).build();

  private final Interval<Integer> EMPTY_CLOSED = builder().startClosed(0).endClosed(0).build();
  private final Interval<Integer> EMPTY_CLOSED_TO_OPEN = builder().startClosed(0).endOpen(0).build();
  private final Interval<Integer> EMPTY_OPEN_TO_CLOSED = builder().startOpen(0).endClosed(0).build();
  private final Interval<Integer> EMPTY_NEGATIVE = builder().startOpen(1).endOpen(-1).build();

  @Override
  protected Stream<Interval<Integer>> getNonEmptyIntervals() {
    return Stream.of(
            infinite,
            CLOSED_START,
            CLOSED_END,
            CLOSED,
            OPEN_TO_CLOSED,
            CLOSED_TO_OPEN,
            OPEN,
            OPEN_DEGENERATE
    );
  }

  @Override
  protected Stream<Interval<Integer>> getEmptyIntervals() {
    return Stream.of(
            EMPTY_CLOSED,
            EMPTY_CLOSED_TO_OPEN,
            EMPTY_OPEN_TO_CLOSED,
            EMPTY_NEGATIVE
    );
  }

  @Override
  Integer[] getEndpointValues() {
    return new Integer[]{-1, 0, 1};
  }


  /**
   * (-inf, 0) | (0, inf) = (-inf, inf)
   */
  @Test
  public void testUnionOfAdjacentOpen() {
    Interval<Integer> lt = Interval.<Integer>builder().endOpen(0).build();
    Interval<Integer> gt = Interval.<Integer>builder().startOpen(0).build();
    allOrderings(lt, gt)
            .map(this::unionList)
            .peek(union -> assertEquals(1, union.size()))
            .map(Collection::iterator)
            .map(Iterator::next)
            .forEach(actual -> assertEquals(infinite, actual));
  }

  /**
   * (-inf, 0] | [0, inf) = (-inf, 0] | [0, inf)
   */
  @Test
  public void testUnionOfAdjacentClosed() {
    Interval<Integer> lt = Interval.<Integer>builder().endClosed(0).build();
    Interval<Integer> gt = Interval.<Integer>builder().startClosed(0).build();
    allOrderings(lt, gt)
            .map(this::unionList)
            .forEach(union -> assertEquals(union, Arrays.asList(lt, gt)));
  }

  /**
   * (-1, 1] | (0, 2] = (-1, 2]
   */
  @Test
  public void testUnionOfOverlapping() {
    Interval<Integer> first = Interval.<Integer>builder().startOpen(-1).endClosed(1).build();
    Interval<Integer> second = Interval.<Integer>builder().startOpen(0).endClosed(2).build();
    Interval<Integer> expected = Interval.<Integer>builder().startOpen(-1).endClosed(2).build();
    allOrderings(first, second)
            .map(this::unionList)
            .peek(union -> assertEquals(1, union.size()))
            .map(Collection::iterator)
            .map(Iterator::next)
            .forEach(actual -> assertEquals(expected, actual));
  }


  /**
   * (-1, 0] | (1, 2] = (-1, 0] | (1, 2]
   */
  @Test
  public void testUnionOfDisjoint() {
    Interval<Integer> first = Interval.<Integer>builder().startOpen(-1).endClosed(0).build();
    Interval<Integer> second = Interval.<Integer>builder().startOpen(1).endClosed(2).build();
    List<Interval<Integer>> expected = Arrays.asList(first, second);
    allOrderings(first, second)
            .map(this::unionList)
            .forEach(union -> assertEquals(expected, union));
  }

  /**
   * (0, 2] | (1, 3) | (3, 4] = (0, 4]
   */
  @Test
  public void testUnionOfOverlappingThree() {
    Interval<Integer> first = Interval.<Integer>builder().startOpen(0).endClosed(2).build();
    Interval<Integer> second = Interval.<Integer>builder().startOpen(1).endOpen(3).build();
    Interval<Integer> third = Interval.<Integer>builder().startOpen(3).endClosed(4).build();
    Interval<Integer> expected = Interval.<Integer>builder().startOpen(0).endClosed(4).build();
    allOrderings(first, second, third)
            .map(this::unionList)
            .peek(union -> assertEquals(1, union.size()))
            .map(Collection::iterator)
            .map(Iterator::next)
            .forEach(actual -> assertEquals(expected, actual));
  }


  /**
   * (0, 1] | [1, 2) | (3, 4] = (0, 1] | [1, 2) | (3, 4]
   */
  @Test
  public void testUnionOfDisjointThree() {
    Interval<Integer> first = Interval.<Integer>builder().startOpen(0).endClosed(1).build();
    Interval<Integer> second = Interval.<Integer>builder().startClosed(1).endOpen(2).build();
    Interval<Integer> third = Interval.<Integer>builder().startOpen(3).endClosed(4).build();
    List<Interval<Integer>> expected = Arrays.asList(first, second, third);
    allOrderings(first, second, third)
            .map(this::unionList)
            .forEach(union -> assertEquals(expected, union));
  }
}
