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

import com.terracottatech.sovereign.impl.compute.CellComparison;
import com.terracottatech.sovereign.impl.compute.DisjunctiveCellComparison;
import com.terracottatech.sovereign.impl.compute.IndexedCellRangePredicate;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.logic.BooleanAnalyzer;
import com.terracottatech.store.logic.Clause;
import com.terracottatech.store.logic.IntervalRecordPredicate;
import com.terracottatech.store.logic.NormalForm;
import com.terracottatech.store.logic.NormalFormCompactor;
import com.terracottatech.store.logic.RecordPredicate;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Optimiser that can handle disjunctions of multiple indexed cells,
 * requiring multi-index look up.
 */
class DisjunctiveSovereignOptimizer<K extends Comparable<K>> implements SovereignOptimizer<K> {

  private final BooleanAnalyzer<Record<K>> booleanAnalyzer = new BooleanAnalyzer<>();
  private final NormalFormCompactor<K> compactor = new NormalFormCompactor<>();
  private final LocatorGenerator<K> generator;

  DisjunctiveSovereignOptimizer(LocatorGenerator<K> generator) {
    this.generator = generator;
  }

  /**
   * Convert the expression to a compact DNF. If it is a contradiction,
   * return an Optimization with skipScan property set to true.
   * Otherwise, check that each conjunctive clause includes
   * at least one predicate on an indexed cell. If this is true,
   * return an Optimization with a CellComparison including best indexes found.
   * Otherwise, return an Optimization with CellComparison set to null,
   * resulting in a full dataset scan.
   */
  @Override
  public Optimization<K> optimize(IntrinsicPredicate<Record<K>> filterPredicate) {
    NormalForm<Record<K>, IntrinsicPredicate<Record<K>>> normalForm = booleanAnalyzer.toNormalForm(filterPredicate, NormalForm.Type.DISJUNCTIVE);
    NormalForm<Record<K>, RecordPredicate<K>> compactNF = compactor.compactNF(normalForm);
    Optimization.Builder<K> builder = Optimization.builder();
    builder = compactNF.isContradiction()
            ? builder.setSkipScan()
            : checkIndexedFields(compactNF, builder);
    return builder.setNormalizedFilterExpression(compactNF.toString())
            .build();
  }

  @SuppressWarnings("rawtypes")
  private Optimization.Builder<K> checkIndexedFields(NormalForm<Record<K>, RecordPredicate<K>> normalForm,
                                                     Optimization.Builder<K> builder) {
    List<Set<IntervalRecordPredicate<K, ? extends Comparable>>> predicateSets = normalForm.clauses()
            .map(this::getIndexedFields)
            .collect(Collectors.toList());
    predicatesForBestCommonIndex(predicateSets)
            .flatMap(this::createCellComparison)
            .ifPresent(builder::setCellComparison);
    return builder;
  }

  @SuppressWarnings("rawtypes")
  private Set<IntervalRecordPredicate<K, ? extends Comparable>> getIndexedFields(Clause<Record<K>, RecordPredicate<K>> clause) {
    return clause.literals()
            .filter(this::hasCellDefinition)
            .map(predicate -> (IntervalRecordPredicate<K, ? extends Comparable>) predicate)
            .filter(this::isIndexed)
            .collect(Collectors.toSet());
  }

  @SuppressWarnings("rawtypes")
  private Optional<List<IntervalRecordPredicate<K, ? extends Comparable>>> predicatesForBestCommonIndex(
          List<Set<IntervalRecordPredicate<K, ? extends Comparable>>> predicateSets) {
    int numberOfClauses = predicateSets.size();
    return groupByCellDefinitions(predicateSets)
            .entrySet()
            .stream()
            .filter(e -> e.getValue().size() == numberOfClauses)
            .min(Comparator.comparing(this::getIndexCoverageEstimate))
            .map(Map.Entry::getValue);
  }

  @SuppressWarnings("rawtypes")
  private Map<CellDefinition<? extends Comparable>, List<IntervalRecordPredicate<K, ? extends Comparable>>> groupByCellDefinitions(
          List<Set<IntervalRecordPredicate<K, ? extends Comparable>>> predicateSets) {
    return predicateSets.stream()
            .flatMap(this::zipIntervalsWithDefinitions)
            .collect(Collectors.toMap(Map.Entry::getKey, this::singletonValue, this::merge));
  }

  @SuppressWarnings("rawtypes")
  private Stream<Map.Entry<CellDefinition<? extends Comparable>, IntervalRecordPredicate<K, ? extends Comparable>>> zipIntervalsWithDefinitions(
          Set<IntervalRecordPredicate<K, ? extends Comparable>> predicateSet) {
    return predicateSet.stream()
            .map(predicate -> new AbstractMap.SimpleEntry<>(predicate.getCellDefinition(), predicate));
  }

  private <T> List<T> singletonValue(Map.Entry<?, T> entry) {
    return Collections.singletonList(entry.getValue());
  }

  private <T> List<T> merge(List<T> first, List<T> next) {
    return Stream.concat(first.stream(), next.stream())
            .collect(Collectors.toList());
  }

  /**
   * Note: CellDefinition is know to be Comparable, hence safe to suppress warning.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private long getIndexCoverageEstimate(Map.Entry<CellDefinition<? extends Comparable>, ?> entry) {
    return generator.getIndexCoverageEstimate(entry.getKey());
  }

  private boolean hasCellDefinition(RecordPredicate<K> literal) {
    return literal instanceof IntervalRecordPredicate;
  }

  private <V extends Comparable<V>> boolean isIndexed(IntervalRecordPredicate<K, V> comparison) {
    CellDefinition<V> cellDefinition = comparison.getCellDefinition();
    return generator.isIndexed(cellDefinition);
  }

  /**
   * Return an Optional wrapping a single object defining the cell definition and the union of
   * index ranges to be scanned (which may consist of one or more ranges).
   * If both the start and end of a range are null (hence the corresponding predicate is a tautology) it is excluded.
   * If the resulting union of ranges is empty, the method returns an empty Optional, resulting in FDS (see see TDB-3724).
   */
  private Optional<CellComparison<K>> createCellComparison(Collection<IntervalRecordPredicate<K, ?>> recordPredicates) {
    List<IndexedCellRangePredicate<K, ?>> ranges = union(recordPredicates)
            .filter(p -> !p.isTautology())
            .map(this::getCellComparisonFragment)
            .collect(Collectors.toList());
    return ranges.isEmpty()
            ? Optional.empty()
            : Optional.of(new DisjunctiveCellComparison<>(ranges));
  }

  /**
   * Find a union of index ranges to scan, to prevent repeated scanning of same records.
   */
  private  <V extends Comparable<V>> Stream<IntervalRecordPredicate<K, V>> union(
          Collection<IntervalRecordPredicate<K, ?>> recordPredicates) {
    Collection<IntervalRecordPredicate<K, V>> cast = cast(recordPredicates);
    return IntervalRecordPredicate.union(cast);
  }

  /**
   * Direct cast of the collection results in a FindBugs warning BC_BAD_CAST_TO_ABSTRACT_COLLECTION
   */
  @SuppressWarnings("unchecked")
  private <V extends Comparable<V>> List<IntervalRecordPredicate<K, V>> cast(Collection<IntervalRecordPredicate<K, ?>> recordPredicates) {
    return recordPredicates.stream()
            .map(p -> (IntervalRecordPredicate<K, V>) p)
            .collect(Collectors.toList());
  }

  private <V extends Comparable<V>> IndexedCellRangePredicate<K, V> getCellComparisonFragment(IntervalRecordPredicate<K, V> recordPredicate) {
    return new IndexedIntervalRecordPredicate<>(recordPredicate);
  }

  /**
   * IndexedCellRangePredicate based on an IntervalRecordPredicate.
   * @param <K> type of the record key.
   * @param <V> type of cell values.
   */
  private static class IndexedIntervalRecordPredicate<K extends Comparable<K>, V extends Comparable<V>> implements IndexedCellRangePredicate<K, V> {

    private final IntervalRecordPredicate<K, V> recordPredicate;
    private final V start;
    private final V end;

    IndexedIntervalRecordPredicate(IntervalRecordPredicate<K, V> recordPredicate) {
      this.recordPredicate = recordPredicate;
      if (recordPredicate.getStart() != null) {
        this.start = recordPredicate.getStart();
        this.end = recordPredicate.getEnd();
      } else {
        this.start = recordPredicate.getEnd();
        this.end = recordPredicate.getStart();
      }
    }

    @Override
    public boolean test(Record<K> record) {
      return recordPredicate.test(record);
    }

    @Override
    public CellDefinition<V> getCellDefinition() {
      return recordPredicate.getCellDefinition();
    }

    @Override
    public ComparisonType operation() {
      return recordPredicate.getOperator();
    }

    @Override
    public V start() {
      return start;
    }

    @Override
    public V end() {
      return end;
    }
  }
}
