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

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.impl.ComparisonType;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * RecordPredicate testing the inclusion of a record's cell definition's value in the pre-defined interval.
 * @param <K> type of record keys.
 * @param <V> type of the value to be extracted from a record.
 */
public class IntervalRecordPredicate<K extends Comparable<K>, V extends Comparable<V>> implements RecordPredicate<K> {

  private final CellDefinition<V> cellDefinition;

  private final Interval<V> interval;

  private final IntervalPredicate<V> comparisonPredicate;

  IntervalRecordPredicate(CellDefinition<V> def, Interval<V> interval) {
    this.cellDefinition = def;
    this.interval = interval;
    comparisonPredicate = IntervalMapper.toPredicate(interval);
  }

  @Override
  public boolean test(Record<K> record) {
    return record.get(cellDefinition).map(comparisonPredicate::test).orElse(false);
  }

  @Override
  public boolean isContradiction() {
    return interval.isEmpty();
  }

  @Override
  public boolean isTautology() {
    return interval.isInfinite();
  }

  /**
   * Return the cell definition if known.
   * @return cell definition if it is known, else null.
   */
  public CellDefinition<V> getCellDefinition() {
    return cellDefinition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IntervalRecordPredicate<?, ?> that = (IntervalRecordPredicate<?, ?>) o;
    return Objects.equals(cellDefinition, that.cellDefinition) &&
            Objects.equals(interval, that.interval);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cellDefinition, interval);
  }

  @Override
  public String toString() {
    return cellDefinition.name() + comparisonPredicate;
  }

  public V getStart() {
    return interval.getStart().getValue();
  }

  public V getEnd() {
    return interval.getEnd().getValue();
  }

  public ComparisonType getOperator() {
    return comparisonPredicate.getOperator();
  }

  /**
   * Get the interval containing all values satisfying these predicate.
   * @return Interval
   */
  Interval<V> getInterval() {
    return interval;
  }

  public static <K extends Comparable<K>, V extends Comparable<V>> Stream<IntervalRecordPredicate<K, V>> union(Collection<IntervalRecordPredicate<K, V>> predicates) {
    return (predicates.isEmpty())
            ? Stream.empty()
            : union(predicates.iterator().next().getCellDefinition(), predicates);
  }

  private static <K extends Comparable<K>, V extends Comparable<V>> Stream<IntervalRecordPredicate<K, V>> union(
          CellDefinition<V> definition,
          Collection<IntervalRecordPredicate<K, V>> predicates) {
    Stream<Interval<V>> intervalStream = predicates.stream()
            .map(IntervalRecordPredicate::getInterval);
    return Intervals.union(intervalStream)
            .map(interval -> new IntervalRecordPredicate<>(definition, interval));
  }
}
