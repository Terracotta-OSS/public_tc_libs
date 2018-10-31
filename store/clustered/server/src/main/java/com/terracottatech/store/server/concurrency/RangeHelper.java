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

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.stream.IntStream;

import static java.lang.Integer.max;
import static java.lang.Integer.min;

public class RangeHelper {
  public static SortedSet<Integer> rangeClosed(int minInclusive, int maxInclusive) {
    if (maxInclusive < minInclusive) {
      return Collections.emptySortedSet();
    } else {
      return new Range(minInclusive, maxInclusive);
    }
  }

  public static SortedSet<Integer> range(int minInclusive, int maxExclusive) {
    if (maxExclusive <= minInclusive) {
      return Collections.emptySortedSet();
    } else {
      return rangeClosed(minInclusive, maxExclusive - 1);
    }
  }

  private static class Range extends AbstractSet<Integer> implements SortedSet<Integer> {

    private final int max;
    private final int min;

    private Range(int min, int max) {
      this.min = min;
      this.max = max;
    }

    @Override
    public boolean contains(Object o) {
      if (o instanceof Integer) {
        int x = ((Integer) o).intValue();
        return min <= x && x <= max;
      } else {
        return false;
      }
    }

    @Override
    public Iterator<Integer> iterator() {
      return IntStream.rangeClosed(min, max).iterator();
    }

    @Override
    public int size() {
      return (max + 1) - min;
    }

    @Override
    public Comparator<? super Integer> comparator() {
      return null;
    }

    @Override
    public SortedSet<Integer> subSet(Integer fromElement, Integer toElement) {
      return rangeClosed(max(fromElement, first()), min(toElement - 1, last()));
    }

    @Override
    public SortedSet<Integer> headSet(Integer toElement) {
      return rangeClosed(min, min(max, toElement - 1));
    }

    @Override
    public SortedSet<Integer> tailSet(Integer fromElement) {
      return rangeClosed(max(min, fromElement), max);
    }

    @Override
    public Integer first() {
      return min;
    }

    @Override
    public Integer last() {
      return max;
    }
  }
}
