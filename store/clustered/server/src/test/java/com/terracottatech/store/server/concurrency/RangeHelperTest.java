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
package com.terracottatech.store.server.concurrency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.SortedSet;
import java.util.stream.IntStream;

import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RangeHelperTest {

  @Test
  public void testClosedRangeIterator() {
    Set<Integer> range = RangeHelper.rangeClosed(0, 10);
    assertThat(range, contains(IntStream.rangeClosed(0, 10).boxed().toArray()));
  }

  @Test
  public void testHighClosedRangeIterator() {
    Set<Integer> range = RangeHelper.rangeClosed(Integer.MAX_VALUE - 10, Integer.MAX_VALUE);
    assertThat(range, contains(IntStream.rangeClosed(Integer.MAX_VALUE - 10, Integer.MAX_VALUE).boxed().toArray()));
  }

  @Test
  public void testClosedRangeContains() {
    Set<Integer> range = RangeHelper.rangeClosed(0, 10);
    IntStream.rangeClosed(0, 10).boxed().map(range::contains).forEach(Assert::assertTrue);
  }

  @Test
  public void testHighClosedRangeContains() {
    Set<Integer> range = RangeHelper.rangeClosed(Integer.MAX_VALUE - 10, Integer.MAX_VALUE);
    IntStream.rangeClosed(Integer.MAX_VALUE - 10, Integer.MAX_VALUE).boxed().map(range::contains).forEach(Assert::assertTrue);
  }

  @Test
  public void testClosedRangeSize() {
    Set<Integer> range = RangeHelper.rangeClosed(0, 10);
    assertThat(range.size(), is(11));
  }

  @Test
  public void testHighClosedRangeSize() {
    Set<Integer> range = RangeHelper.rangeClosed(Integer.MAX_VALUE - 10, Integer.MAX_VALUE);
    assertThat(range.size(), is(11));
  }


  @Test
  public void testRangeIterator() {
    Set<Integer> range = RangeHelper.range(0, 10);
    assertThat(range, contains(IntStream.range(0, 10).boxed().toArray()));
  }

  @Test
  public void testHighRangeIterator() {
    Set<Integer> range = RangeHelper.range(Integer.MAX_VALUE - 10, Integer.MAX_VALUE);
    assertThat(range, contains(IntStream.range(Integer.MAX_VALUE - 10, Integer.MAX_VALUE).boxed().toArray()));
  }

  @Test
  public void testRangeContains() {
    Set<Integer> range = RangeHelper.range(0, 10);
    IntStream.range(0, 10).boxed().map(range::contains).forEach(Assert::assertTrue);
  }

  @Test
  public void testHighRangeContains() {
    Set<Integer> range = RangeHelper.range(Integer.MAX_VALUE - 10, Integer.MAX_VALUE);
    IntStream.range(Integer.MAX_VALUE - 10, Integer.MAX_VALUE).boxed().map(range::contains).forEach(Assert::assertTrue);
  }

  @Test
  public void testRangeSize() {
    Set<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.size(), is(10));
  }

  @Test
  public void testHighRangeSize() {
    Set<Integer> range = RangeHelper.range(Integer.MAX_VALUE - 10, Integer.MAX_VALUE);
    assertThat(range.size(), is(10));
  }

  @Test
  public void testEmptyHeadSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.headSet(0), empty());
  }

  @Test
  public void testPartialHeadSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.headSet(3), contains(IntStream.range(0, 3).boxed().toArray()));
  }

  @Test
  public void testFullHeadSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.headSet(20), contains(IntStream.range(0, 10).boxed().toArray()));
  }

  @Test
  public void testEmptyTailSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.tailSet(10), empty());
  }

  @Test
  public void testPartialTailSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.tailSet(7), contains(IntStream.range(7, 10).boxed().toArray()));
  }

  @Test
  public void testFullTailSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.tailSet(-10), contains(IntStream.range(0, 10).boxed().toArray()));
  }

  @Test
  public void testEmptySubSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.subSet(10, 12), empty());
  }

  @Test
  public void testPartialSubSet() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.subSet(3, 9), contains(IntStream.range(3, 9).boxed().toArray()));
  }

  @Test
  public void testFirst() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.first(), is(0));
  }

  @Test
  public void testLast() {
    SortedSet<Integer> range = RangeHelper.range(0, 10);
    assertThat(range.last(), is(9));
  }
}
