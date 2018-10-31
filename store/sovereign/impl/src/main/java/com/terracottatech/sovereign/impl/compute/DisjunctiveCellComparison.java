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

import com.terracottatech.sovereign.plan.IndexedCellRange;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.impl.ComparisonType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Used with expressions normalised to DNF. Contains potentially overlapping ranges on
 * the same or different indexed cells, joined by OR operator.
 * Prevents duplicate results by excluding overlapping records,
 * see {@link DisjunctiveIndexedCellRangePredicate#test(Record)}.
 */
public class DisjunctiveCellComparison<K extends Comparable<K>>
        implements CellComparison<K> {

  private final List<IndexedCellRangePredicate<K, ? extends Comparable<?>>> ranges = new ArrayList<>();

  public DisjunctiveCellComparison(List<IndexedCellRangePredicate<K, ?>> ranges) {
    createNonOverlappingFragments(ranges);
  }

  private void createNonOverlappingFragments(List<IndexedCellRangePredicate<K, ? extends Comparable<?>>> ranges) {
    int size = ranges.size();
    for (int i = 0; i < size; i++) {
      IndexedCellRangePredicate<K, ?> nextFragment = indexedCellRangePredicate(ranges.get(i), ranges.subList(i + 1, size));
      this.ranges.add(nextFragment);
    }
  }

  private <V extends Comparable<V>> IndexedCellRangePredicate<K, V> indexedCellRangePredicate(IndexedCellRangePredicate<K, V> currentRange,
                                                                                              List<IndexedCellRangePredicate<K, ?>> subsequentRanges) {
    return new DisjunctiveIndexedCellRangePredicate<>(currentRange, subsequentRanges);
  }

  @Override
  public int countIndexRanges() {
    return ranges.size();
  }

  @Override
  public Iterator<IndexedCellRangePredicate<K, ? extends Comparable<?>>> indexRangeIterator() {
    return ranges.iterator();
  }

  @Override
  public List<? extends IndexedCellRange<?>> indexRanges() {
    return ranges;
  }

  /**
   * Tests a record against its current range and all subsequent ranges, to prevent duplicate results.
   * @param <K>
   * @param <V>
   */
  private static class DisjunctiveIndexedCellRangePredicate<K extends Comparable<K>, V extends Comparable<V>>
          implements IndexedCellRangePredicate<K, V> {

    private final IndexedCellRangePredicate<K, V> currentRange;
    private final List<IndexedCellRangePredicate<K, ?>> subsequentRanges;

    private DisjunctiveIndexedCellRangePredicate(IndexedCellRangePredicate<K, V> currentRange, List<IndexedCellRangePredicate<K, ?>> subsequentRanges) {
      this.currentRange = currentRange;
      this.subsequentRanges = subsequentRanges;
    }

    /**
     * Prevents duplicate results.
     * @param record record to be tested.
     * @return true iff the record is within the current range and not within any subsequent range.
     */
    @Override
    public boolean test(Record<K> record) {
      return currentRange.test(record);
    }

    @Override
    public boolean overlaps(Record<K> record) {
      for (IndexedCellRangePredicate<K, ?> subsequent : subsequentRanges) {
        if (subsequent.test(record)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public CellDefinition<V> getCellDefinition() {
      return currentRange.getCellDefinition();
    }

    @Override
    public ComparisonType operation() {
      return currentRange.operation();
    }

    @Override
    public V start() {
      return currentRange.start();
    }

    @Override
    public V end() {
      return currentRange.end();
    }
  }
}
