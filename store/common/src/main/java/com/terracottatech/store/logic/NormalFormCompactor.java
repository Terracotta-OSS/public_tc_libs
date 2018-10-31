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
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.impl.CellExtractor;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.GatedComparison;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.terracottatech.store.logic.NormalForm.Type.DISJUNCTIVE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toSet;

/**
 * Converts a DNF to a more compact representation as follows:
 * <ol>
 *   <li>If the input is a contradiction, return (false). If it is tautology, return (true).</li>
 *   <li>Otherwise, in each conjunctive clause:
 *   <ol>
 *     <li>
 *       Find comparisons on cell definitions: e.g. the first five predicates in the following clause:<br>
 *       {@code (x1 > 1) & (x1 <= 2) & (x1 < 3) & (x2 > 4) & (x2 = 3) & (x2 exists)}
 *     </li>
 *     <li>
 *       Group them by cell definitions:<br>
 *       {@code x1: (x1 > 1), (x1 <= 2), (x1 < 3)}<br>
 *       {@code x2: (x2 > 4), (x2 = 3)}
 *     </li>
 *     <li>
 *       Convert comparisons to intervals:<br>
 *         {@code (x1 > 1) -> [1, inf)}<br>
 *         {@code (x1 <= 2) -> (-inf, 2)}<br>
 *         {@code (x1 < 3) -> (-inf, 3]}<br>
 *         {@code (x2 > 4) -> [4, inf)}<br>
 *         {@code (x2 = 3) -> (3, 3)}
 *     </li>
 *     <li>
 *       Find the intersection of intervals for each group:<br>
 *       {@code [1, inf) & (-inf, 2) & (-inf, 3) -> [1, 2)}<br>
 *       {@code [4, inf) & (3, 3) -> [4, 3)} <br>
 *       note the negative intersection [4, 3)!
 *     </li>
 *     <li>Convert the intersection to a single predicate (it may be a conjunction):<br>
 *       {@code [1, 2) -> x1 > 1 & x1 <= 2 }<br>
 *       {@code [4, 3) -> false}<br>
 *       note that a negative or empty interval is converted to a contradiction
 *       i.e. the predicate always returns false.
 *     </li>
 *     <li>
 *       Replace each group of gated comparisons with the single "compacted" predicate:<br>
 *       {@code (x1 > 1) & (x1 <= 2) & (x1 < 3) & (x2 > 4) & (x2 = 3) & (x2 exists) ->  (x1 > 1 & x1 <= 2) & false & (x2 exists) }
 *     </li>
 *   </ol>
 *   </li>
 *   <li>
 *     Construct a DNF from conjuctive clauses "compacted" in the previous step,
 *     but discard a clause if it contains at least one contradiction:<br>
 *     {@code (x1 > 1 & x1 <= 2) & false & (x2 exists) -> ()}
 *   </li>
 *   <li>
 *    If all clauses are discarded, return (false):<br>
 *    {@code () -> false}
 *   </li>
 *   <li>
 *    Find groups of clauses consisting of a single comparison on the same cell, and:
 *      <ol>
 *       <li>Replace each group with the unions which may or may not be a single interval:<br>
 *       {@code (x < 1) | (x > 0) -> (x >= inf & x <= inf) }<br>
 *       {@code (x2 > 0 & x2 < 2) | (x2 > 1 & x2 < 3) -> (x > 0 & x2 < 3) }<br>
 *       {@code (x2 > 0 & x2 < 1) | (x2 > 2 & x2 < 3) -> (x2 > 0 & x2 < 1) | (x2 > 2 & x2 < 3) }<br>
 *       </li>
 *       <li>
 *        Discard infinite intervals as tautologies:<br>
 *        {@code (x >= inf & x <= inf) -> true }<br>
 *        Note that currently the compacted clause is only used to choose indexes for scan,
 *        so cell existence in records is irrelevant.
 *       </li>
 *     </ol>
 *     </li>
 *    <li>
 *     If all clauses are discarded, return (true):<br>
 *     {@code () -> true}
 *    </li>
 *    <li>Return the DNF constructed.</li>
 * </ol>
 * @param <K> the type of the record keys.
 */
public class NormalFormCompactor<K extends Comparable<K>> {

  /**
   * Converts a DNF to a more compact representation (see the class documentation).
   * @param normalForm NF containing IntrinsicPredicates on Records as literals.
   * @return if the NF is disjunctive, return a compact representation of the DNF, containing RecordPredicates as literals. If the NF is conjunctive, throws IllegalArgumentException.
   */
  public NormalForm<Record<K>, RecordPredicate<K>> compactNF(@Nonnull NormalForm<Record<K>, IntrinsicPredicate<Record<K>>> normalForm)
          throws IllegalArgumentException {
    switch (normalForm.getType()) {
      case CONJUNCTIVE:
        throw new IllegalArgumentException("CNF range compaction is currently not supported.");
      case DISJUNCTIVE:
        return compactDNF(normalForm);
      default:
        throw new IllegalArgumentException("Unknown NF type.");
    }
  }

  /**
   * If the input is a contradiction, return (false). If it is tautology, return (true).
   * Otherwise, return a compacted DNF.
   * @param dnf DNF
   * @return compact DNF.
   */
  private NormalForm<Record<K>, RecordPredicate<K>> compactDNF(NormalForm<Record<K>, IntrinsicPredicate<Record<K>>> dnf) {
    if (dnf.isContradiction()) {
      return NormalForm.constant(DISJUNCTIVE, false);
    } else if (dnf.isTautology()) {
      return NormalForm.constant(DISJUNCTIVE, true);
    } else {
      return compactConjunctions(dnf);
    }
  }

  /**
   * Compact each conjunctive clause. Discard compacted clauses containing contradiction.
   * If all clauses are discarded, return (false). Otherwise, find unions of singleton clauses
   * consisting of same cell comparisons, and discard tautologies.
   * If all clauses are discarded, return (true). Otherwise, return the DNF constructed.
   * @param dnf DNF
   * @return compact DNF
   */
  private NormalForm<Record<K>, RecordPredicate<K>> compactConjunctions(NormalForm<Record<K>, IntrinsicPredicate<Record<K>>> dnf) {
    NormalForm.Builder<Record<K>, RecordPredicate<K>> builder = NormalForm.builder(DISJUNCTIVE);
    Set<Set<RecordPredicate<K>>> compactedClauses = dnf.clauses()
            .map(this::compactConjunction)
            .filter(this::containsNoContradiction)
            .collect(Collectors.toSet());
    if (compactedClauses.isEmpty()) {
      return NormalForm.constant(DISJUNCTIVE, false);
    }
    compactAcrossClauses(compactedClauses)
            .filter((Set<RecordPredicate<K>> predicates) -> !predicates.stream().allMatch(RecordPredicate::isTautology))
            .forEach(builder::addClause);
    NormalForm<Record<K>, RecordPredicate<K>> normalForm = builder.build();
    return normalForm.clauses().count() > 0
            ? normalForm
            : NormalForm.constant(DISJUNCTIVE, true);
  }

  /**
   * Find singleton clauses consisting of same cell comparisons replace with
   * their unions, and discard tautologies (see class documentation).
   */
  private Stream<Set<RecordPredicate<K>>> compactAcrossClauses(Collection<Set<RecordPredicate<K>>> clauses) {
    Map<Boolean, List<Set<RecordPredicate<K>>>> partitionBySize = clauses
            .stream()
            .collect(partitioningBy(this::isCompactableSingletonSet));

    Stream<Set<RecordPredicate<K>>> singletons = partitionBySize.get(true).stream();
    Stream<Set<RecordPredicate<K>>> compactedSingletons = compactSameCellSingletonClauses(singletons);

    Stream<Set<RecordPredicate<K>>> compounds = partitionBySize.get(false).stream();

    return Stream.concat(compactedSingletons, compounds);
  }

  /**
   * Check that the clause is singleton and the single predicate is a comparison.
   */
  private boolean isCompactableSingletonSet(Set<RecordPredicate<K>> predicates) {
    return predicates.size() == 1 && predicates.iterator().next() instanceof IntervalRecordPredicate;
  }

  /**
   * Replace singleton clauses consisting of same cell comparisons replace with their unions.
   */
  @SuppressWarnings("unchecked")
  private <V extends Comparable<V>> Stream<Set<RecordPredicate<K>>> compactSameCellSingletonClauses(Stream<Set<RecordPredicate<K>>> singletons) {
    return singletons.map(Set::iterator)
            .map(Iterator::next)
            .map(p -> (IntervalRecordPredicate<K, V>) p)
            .collect(groupingBy(IntervalRecordPredicate::getCellDefinition))
            .entrySet()
            .stream()
            .flatMap(e -> compactSameCellPredicates(e.getKey(), e.getValue()))
            .map(Collections::singleton);
  }

  /**
   * Find the union of same cell comparisons which may of may not be a single interval.
   */
  private <V extends Comparable<V>> Stream<IntervalRecordPredicate<K, V>> compactSameCellPredicates(CellDefinition<V> def,
                                                                                                    List<IntervalRecordPredicate<K, V>> predicates) {
    Stream<Interval<V>> intervalStream = predicates.stream()
            .map(IntervalRecordPredicate::getInterval);
    Stream<Interval<V>> union = Intervals.union(intervalStream);
    return union.map(interval -> new IntervalRecordPredicate<K, V>(def, interval));
  }

  /**
   * Check a set of predicates for contradictions.
   * @param recordPredicates RecordPredicates.
   * @return true if a contradictory predicate is found, else false.
   */
  private boolean containsNoContradiction(Set<RecordPredicate<K>> recordPredicates) {
    return recordPredicates.stream().noneMatch(RecordPredicate::isContradiction);
  }

  /**
   * Compact a conjunctive clause (see class documentation). Classify the predicates by type,
   * then replace the set of cell comparisons with its compact version, then return its
   * union with the set of predicates which are not cell comparisons.
   * @param clause conjunctive clause
   * @return compact set of predicates.
   */
  @SuppressWarnings("unchecked")
  private <V extends Comparable<V>>  Set<RecordPredicate<K>> compactConjunction(Clause<Record<K>, IntrinsicPredicate<Record<K>>> clause) {
    Map<Boolean, List<IntrinsicPredicate<Record<K>>>> partitionByType = clause.literals()
            .collect(partitioningBy(this::checkTypes));
    Stream<RecordPredicate<K>> nonCompactables = toRecordPredicates(partitionByType.get(false));
    Stream<GatedComparison<Record<K>, V>> compactables = partitionByType.get(true)
            .stream()
            .map(p -> (GatedComparison<Record<K>, V>) p);
    Set<RecordPredicate<K>> compacted = compactComparisons(compactables);
    return Stream.concat(nonCompactables, compacted.stream()).collect(toSet());
  }

  /**
   * Check whether an IntrinsicPredicate is a cell comparison.
   * @param predicate IntrinsicPredicate
   * @return true if predicate is of type GatedComparison whose left is a CellExtractor and right is a Constant
   * and its value is Comparable; else false.
   */
  private boolean checkTypes(IntrinsicPredicate<Record<K>> predicate) {
    if (predicate instanceof GatedComparison) {
      GatedComparison<Record<K>, ?> gp = (GatedComparison<Record<K>, ?>) predicate;
      return gp.getLeft() instanceof CellExtractor
              && Comparable.class.isAssignableFrom(((CellExtractor) gp.getLeft()).extracts().type().getJDKType())
              && gp.getRight() instanceof Constant
              && ((Constant) gp.getRight()).getValue() instanceof Comparable;
    } else {
      return false;
    }
  }

  /**
   * Convert a set of IntrinsicPredicate which are not cell comparisons to RecordPredicates
   * which delegate the testing to the original predicates.
   * @param intrinsics Collection of IntrinsicPredicates
   * @return Stream of RecordPredicates
   */
  private Stream<RecordPredicate<K>> toRecordPredicates(Collection<IntrinsicPredicate<Record<K>>> intrinsics) {
    return intrinsics.stream().map(DefaultRecordPredicate::new);
  }

  /**
   * Group cell comparisons by cell definitions, then compact each group to a single predicate
   * (see class documentation).
   * @param comparisons stream of GatedComparisons
   * @return set of RecordPredicates
   */
  private <V extends Comparable<V>> Set<RecordPredicate<K>> compactComparisons(Stream<GatedComparison<Record<K>, V>> comparisons) {
    Stream<RecordPredicate<K>> recordPredicateStream = comparisons
            .collect(groupingBy(this::getCellDefinition))
            .entrySet()
            .stream()
            .map(e -> compactConstantComparisons(e.getKey(), e.getValue()));
    return recordPredicateStream.collect(toSet());
  }

  /**
   * Extract a cell definition from a cell comparison.
   * <p>
   * Note: the types are already checked in {@code checkTypes before calling this method}
   *
   * @param literal GatedComparison
   * @param <V> data type of cell.
   * @return CellDefinition
   */
  @SuppressWarnings("unchecked")
  private <V extends Comparable<V>> CellDefinition<V> getCellDefinition(GatedComparison<Record<K>, V> literal) {
    IntrinsicFunction<Record<K>, Optional<V>> left = literal.getLeft();
    CellExtractor<V> extractor = (CellExtractor<V>) left;
    return extractor.extracts();
  }

  /**
   * Convert a group of comparisons on the same cell to a single interval based RecordPredicate
   * (see class documentation).
   * @param def definition of the cell.
   * @param comparisons list of GatedComparisons on the cell.
   * @param <V> data type of the cell.
   * @return {@link IntervalRecordPredicate} based on the intersection of the comparison intervals.
   */
  private <V extends Comparable<V>> IntervalRecordPredicate<K, V> compactConstantComparisons(CellDefinition<V> def, List<GatedComparison<Record<K>, V>> comparisons) {
    Stream<Interval<V>> intervalStream = comparisons.stream()
            .map(NormalFormCompactor::toInterval);
    return intervalStream
            .reduce(Intervals::intersection)
            .map(interval -> new IntervalRecordPredicate<K, V>(def, interval))
            .orElseThrow(() -> new IllegalStateException("groupingBy output should contain no empty lists."));
  }

  /**
   * Convert a cell comparison to an interval
   * (see class documentation).
   * <p>
   * Note: the types are already checked in {@code checkTypes before calling this method}
   * @param comparison GatedComparison
   * @param <V> data type of the cell and of the interval.
   * @return interval including all elements satisfying the comparison.
   */
  private static <V extends Comparable<V>> Interval<V> toInterval(GatedComparison<?, ?> comparison) {
    @SuppressWarnings("unchecked")
    Constant<?, V> constant = (Constant<?, V>) comparison.getRight();
    V value = constant.getValue();
    ComparisonType comparisonType = comparison.getComparisonType();
    return IntervalMapper.toInterval(comparisonType, value);
  }
}
