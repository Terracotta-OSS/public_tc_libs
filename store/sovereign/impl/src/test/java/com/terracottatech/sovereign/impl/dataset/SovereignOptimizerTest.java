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

import com.terracottatech.sovereign.plan.IndexedCellSelection;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.terracottatech.sovereign.impl.compute.CellComparison;
import com.terracottatech.sovereign.plan.IndexedCellRange;
import com.terracottatech.sovereign.plan.Plan;
import com.terracottatech.sovereign.plan.SortedIndexPlan;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.CellExtractor;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.GatedComparison;
import com.terracottatech.store.intrinsics.impl.NonGatedComparison;
import com.terracottatech.store.intrinsics.impl.RecordKey;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.EQ;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.NEQ;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

/**
 * Basic expression tests for {@link SovereignOptimizer}.
 */
@SuppressWarnings("Duplicates")
public class SovereignOptimizerTest {

  @Mock
  private LocatorGenerator<String> locatorGenerator;

  @Mock
  private LocatorGenerator<String> singleIndexLocatorGenerator;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(locatorGenerator.isIndexed(any()))
            .thenReturn(true);
    when(locatorGenerator.getIndexCoverageEstimate(any()))
            .thenReturn(1000L);
    when(singleIndexLocatorGenerator.isIndexed(argThat(cellDefinition -> cellDefinition.name().equals("cell1"))))
            .thenReturn(true);
  }

  private CellComparison<String> optimize(IntrinsicPredicate<Record<String>> expression, StreamPlanTracker tracker) {
    return optimize(expression, tracker, locatorGenerator);
  }

  private CellComparison<String> optimize(IntrinsicPredicate<Record<String>> expression, StreamPlanTracker tracker,
                                                                   LocatorGenerator<String> generator) {
    tracker.startPlanning();
    tracker.setUsedFilterExpression(expression.toString());
    Optimization<String> result = SovereignOptimizerFactory.newInstance(generator).optimize(expression);
    tracker.setResult(result);
    tracker.endPlanning();
    return result.getCellComparison();
  }

  @Test
  public void testNonGatedPredicate() {
    IntrinsicPredicate<Record<String>> expression = getKeyBasedPredicate();

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    assertThat(tracker.isSortedCellIndexUsed(), is(false));
  }

  @Test
  public void testNonGatedPredicateAndCellPredicate() {
    IntrinsicPredicate<Record<String>> keyPredicate = getKeyBasedPredicate();
    IntrinsicPredicate<Record<String>> cellPredicate = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(keyPredicate, cellPredicate);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);
    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));
    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().get(0);
    assertThat(indexedCellRange.operation(), is(LESS_THAN));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("intCell"));
  }

  @Test
  public void testNonGatedPredicateOrCellPredicate() {
    IntrinsicPredicate<Record<String>> keyPredicate = getKeyBasedPredicate();
    IntrinsicPredicate<Record<String>> cellPredicate = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(keyPredicate, cellPredicate);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    assertThat(tracker.isSortedCellIndexUsed(), is(false));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.FULL_DATASET_SCAN));
  }

  @Test
  public void testCellPredicateOrNonGatedPredicate() {
    IntrinsicPredicate<Record<String>> keyPredicate = getKeyBasedPredicate();
    IntrinsicPredicate<Record<String>> cellPredicate = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicate, keyPredicate);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    assertThat(tracker.isSortedCellIndexUsed(), is(false));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.FULL_DATASET_SCAN));
  }

  @Test
  public void testNegatedNonGatedPredicateOrCellPredicate() {
    IntrinsicPredicate<Record<String>> keyPredicate = getKeyBasedPredicate();
    IntrinsicPredicate<Record<String>> cellPredicate = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(keyPredicate, cellPredicate).negate();

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);
    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));
    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().get(0);
    assertThat(indexedCellRange.operation(), is(GREATER_THAN_OR_EQUAL));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(tracker.isSortedCellIndexUsed(), is(true));
  }

  @Test
  public void testCellPredicateClosedRangeConjunction() {
    IntrinsicPredicate<Record<String>> cellPredicateLt10 = getCellBasedPredicate("intCell", LESS_THAN, 10);
    IntrinsicPredicate<Record<String>> cellPredicateGt5 = getCellBasedPredicate("intCell", GREATER_THAN, 5);
    IntrinsicPredicate<Record<String>> cellPredicateGt0 = getCellBasedPredicate("intCell", GREATER_THAN, 0);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cellPredicateLt10,
            new BinaryBoolean.And<>(cellPredicateGt5, cellPredicateGt0));

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));

    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().get(0);
    ComparisonType operation = indexedCellRange.operation();
    assertThat(operation, is(GREATER_THAN));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(indexedCellRange.end(), is(10));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("intCell"));
  }

  @Test
  public void testCellPredicateMultiRangeDisjunction() {
    IntrinsicPredicate<Record<String>> cellPredicateLt5 = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> cellPredicateGt10 = getCellBasedPredicate("intCell", GREATER_THAN, 10);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateLt5, cellPredicateGt10);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(2));

    List<? extends IndexedCellRange<?>> indexedCellRanges = bestCellComparison.indexRanges();

    Optional<? extends IndexedCellRange<?>> optRangeLt5 = indexedCellRanges.stream()
            .filter(range -> range.start().equals(5))
            .findFirst();
    assertTrue(optRangeLt5.isPresent());
    IndexedCellRange<?> rangeLt5 = optRangeLt5.get();
    assertThat(rangeLt5.operation(), is(LESS_THAN));


    Optional<? extends IndexedCellRange<?>> optRangeGt10 = indexedCellRanges.stream()
            .filter(range -> range.start().equals(10))
            .findFirst();
    assertTrue(optRangeGt10.isPresent());
    IndexedCellRange<?> rangeGt10 = optRangeGt10.get();
    assertThat(rangeGt10.operation(), is(GREATER_THAN));
    assertThat(rangeGt10.start(), is(10));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_MULTI_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("intCell"));
  }

  @Test
  public void testCellPredicateMultiRangeDisjunctionOfConjunctions() {
    IntrinsicPredicate<Record<String>> cellPredicateLt5 = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> cellPredicateGt10 = getCellBasedPredicate("intCell", GREATER_THAN, 10);
    IntrinsicPredicate<Record<String>> cellPredicateLt20 = getCellBasedPredicate("intCell", LESS_THAN, 20);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateLt5,
            new BinaryBoolean.And<>(cellPredicateGt10, cellPredicateLt20));

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(2));
    List<? extends IndexedCellRange<?>> indexedCellRanges = bestCellComparison.indexRanges();

    Optional<? extends IndexedCellRange<?>> optRangeLt5 = indexedCellRanges.stream()
            .filter(range -> range.start().equals(5))
            .findFirst();
    assertTrue(optRangeLt5.isPresent());
    IndexedCellRange<?> rangeLt5 = optRangeLt5.get();
    assertThat(rangeLt5.operation(), is(LESS_THAN));
    assertThat(rangeLt5.end(), is(nullValue()));

    Optional<? extends IndexedCellRange<?>> optRangeGt10 = indexedCellRanges.stream()
            .filter(range -> range.start().equals(10))
            .findFirst();
    assertTrue(optRangeGt10.isPresent());
    IndexedCellRange<?> rangeGt10 = optRangeGt10.get();
    assertThat(rangeGt10.operation(), is(GREATER_THAN));
    assertThat(rangeGt10.end(), is(20));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_MULTI_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("intCell"));
  }

  @Test
  public void testIndexedCellPredicateAndNonIndexedCellPredicate() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell2);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            singleIndexLocatorGenerator);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));
    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().get(0);
    assertThat(indexedCellRange.operation(), is(EQ));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell1"));
  }

  @Test
  public void testAsymmetricIndexedCellsPredicate() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell2);

    @SuppressWarnings("unchecked")
    LocatorGenerator<String> asymmetricIndexLocatorGenerator = Mockito.mock(LocatorGenerator.class);

    when(asymmetricIndexLocatorGenerator.isIndexed(any())).thenReturn(true);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell1", 100L);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell2", 1000L);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            asymmetricIndexLocatorGenerator);

    assertThat(bestCellComparison, is(notNullValue()));
    Matcher<String> eitherCellName = anyOf(is("cell1"), is("cell2"));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));
    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().get(0);
    assertThat(indexedCellRange.operation(), is(EQ));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), eitherCellName);
  }

  private void setIndexCoverageEstimate(LocatorGenerator<String> locatorGenerator, String forName, long size) {
    when(locatorGenerator.getIndexCoverageEstimate(argThat(cellDefinition -> Optional.ofNullable(cellDefinition)
            .map(CellDefinition::name)
            .map(forName::equals)
            .orElse(false)
    ))).thenReturn(size);
  }

  @Test
  public void testIndexedCellPredicateOrNonIndexedCellPredicate() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateCell1, cellPredicateCell2);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            singleIndexLocatorGenerator);

    assertThat(bestCellComparison, is(nullValue()));
    assertThat(tracker.isSortedCellIndexUsed(), is(false));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.FULL_DATASET_SCAN));
  }

  @Test
  public void testRedundantOr() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateCell1, cellPredicateCell1);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            singleIndexLocatorGenerator);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));
    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().get(0);
    assertThat(indexedCellRange.operation(), is(EQ));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell1"));
  }

  @Test
  public void testRedundantOptimisedExpression() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell3 = getCellBasedPredicate("cell3", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell2),
            new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell3));

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            singleIndexLocatorGenerator);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));

    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().iterator().next();
    assertThat(indexedCellRange.operation(), is(EQ));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(indexedCellRange.end(), is(5));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell1"));
  }

  @Test
  public void testContradictoryEquality() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell1", EQ, 6);
    IntrinsicPredicate<Record<String>> cellPredicateCell3 = getCellBasedPredicate("cell1", EQ, 7);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cellPredicateCell1,
            new BinaryBoolean.And<>(cellPredicateCell2, cellPredicateCell3));

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SKIP_SCAN));
  }

  @Test
  public void testContradictoryEqualsAndNotEquals() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell1", NEQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell2);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SKIP_SCAN));
  }


  @Test
  public void testContradictoryEqualsAndNotEqualsForBytes() {
    BytesCellDefinition def = CellDefinition.defineBytes("test");
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate(def, EQ, new byte[]{});
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate(def, NEQ, new byte[]{});
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell2);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.FULL_DATASET_SCAN));
  }

  @Ignore("Until the consistency of multi-index scan is guaranteed.")
  @Test
  public void testIndexedCellPredicateOrIndexedCellPredicate() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateCell1, cellPredicateCell2);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(bestCellComparison.countIndexRanges(), is(2));
    assertThat(getCellDefinitionNames(bestCellComparison), containsInAnyOrder("cell1", "cell2"));
    bestCellComparison.indexRanges()
            .forEach(indexedCellRange -> {
              assertThat(indexedCellRange.operation(), is(EQ));
              assertThat(indexedCellRange.start(), is(5));
              assertThat(tracker.isSortedCellIndexUsed(), is(true));
            });
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_MULTI_SCAN));
    assertEquals(bestCellComparison, ((SortedIndexPlan) executingPlan).indexedCellSelection());
  }

  @Ignore("Until the consistency of multi-index scan is guaranteed.")
  @Test
  public void testIndexedCellPredicateOrConjunctionOfIndexedCellPredicates() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell3 = getCellBasedPredicate("cell3", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateCell1,
            new BinaryBoolean.And<>(cellPredicateCell2, cellPredicateCell3));

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(bestCellComparison.countIndexRanges(), is(2));
    Set<String> names = getCellDefinitionNames(bestCellComparison);
    assertThat(names, hasItem("cell1"));
    assertThat(names, hasItem(anyOf(is("cell2"), is("cell3"))));
    bestCellComparison.indexRanges()
            .forEach(indexedCellRange -> {
              assertThat(indexedCellRange.operation(), is(EQ));
              assertThat(indexedCellRange.start(), is(5));
              assertThat(tracker.isSortedCellIndexUsed(), is(true));
            });
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_MULTI_SCAN));
    assertEquals(bestCellComparison, ((SortedIndexPlan) executingPlan).indexedCellSelection());
  }

  @Test
  public void testCommonIndexedCellInClauses() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1lt = getCellBasedPredicate("cell1", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell1gt = getCellBasedPredicate("cell1", GREATER_THAN, 0);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell3 = getCellBasedPredicate("cell3", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(new BinaryBoolean.And<>(cellPredicateCell1gt, cellPredicateCell1lt), cellPredicateCell2),
            new BinaryBoolean.And<>(cellPredicateCell1gt, new BinaryBoolean.And<>(cellPredicateCell2, cellPredicateCell3)));

    @SuppressWarnings("unchecked")
    LocatorGenerator<String> asymmetricIndexLocatorGenerator = Mockito.mock(LocatorGenerator.class);

    when(asymmetricIndexLocatorGenerator.isIndexed(any())).thenReturn(true);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell1", 100L);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell2", 75L);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell3", 50L);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            asymmetricIndexLocatorGenerator);
    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(bestCellComparison.countIndexRanges(), is(1));

    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().iterator().next();
    assertThat(indexedCellRange.getCellDefinition().name(), is("cell2"));
    assertThat(indexedCellRange.operation(), is(EQ));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(indexedCellRange.end(), is(5));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell2"));
  }

  @Test
  public void testIndexedCellPredicateOrLogicalContradiction() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2eq5 = getCellBasedPredicate("cell2", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateCell1,
            new BinaryBoolean.And<>(cellPredicateCell2eq5, cellPredicateCell2eq5.negate()));

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));
    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().get(0);
    assertThat(indexedCellRange.operation(), is(EQ));
    assertThat(indexedCellRange.start(), is(5));
    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell1"));
  }

  @Test
  public void testLogicalContradiction() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("cell1", EQ, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell1.negate());

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SKIP_SCAN));
  }

  @Test
  public void testAndOverOrOverAnd() {
    IntrinsicPredicate<Record<String>> gte100 = getCellBasedPredicate("intCell", GREATER_THAN_OR_EQUAL, 100);
    IntrinsicPredicate<Record<String>> lt200 = getCellBasedPredicate("intCell", LESS_THAN, 200);
    IntrinsicPredicate<Record<String>> gt750 = getCellBasedPredicate("intCell", GREATER_THAN, 750);
    IntrinsicPredicate<Record<String>> lte1000 = getCellBasedPredicate("intCell", LESS_THAN_OR_EQUAL, 1000);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(gte100,
            new BinaryBoolean.Or<>(lt200, new BinaryBoolean.And<>(gt750, lte1000)));

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(2));
    List<? extends IndexedCellRange<?>> indexedCellRanges = bestCellComparison.indexRanges();

    Optional<? extends IndexedCellRange<?>> optGte100range = indexedCellRanges.stream()
            .filter(range -> range.start().equals(100))
            .findFirst();
    assertTrue(optGte100range.isPresent());
    IndexedCellRange<?> gte100range = optGte100range.get();
    assertThat(gte100range.operation(), is(GREATER_THAN_OR_EQUAL));
    assertThat(gte100range.end(), is(200));

    Optional<? extends IndexedCellRange<?>> optGt750range = indexedCellRanges.stream()
            .filter(range -> range.start().equals(750))
            .findFirst();
    assertTrue(optGt750range.isPresent());
    IndexedCellRange<?> gt750range = optGt750range.get();
    assertThat(gt750range.operation(), is(GREATER_THAN));
    assertThat(gt750range.start(), is(750));
    assertThat(gt750range.end(), is(1000));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_MULTI_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("intCell"));
  }

  @Test
  public void testInfiniteUnion() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("intCell", GREATER_THAN, 0);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateCell1, cellPredicateCell2);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.FULL_DATASET_SCAN));
  }

  @Test
  public void testFiniteUnion() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("intCell", GREATER_THAN, 0);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("intCell", LESS_THAN, 10);
    IntrinsicPredicate<Record<String>> cellPredicateCell3 = getCellBasedPredicate("intCell", GREATER_THAN, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell4 = getCellBasedPredicate("intCell", LESS_THAN, 15);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(cellPredicateCell1, cellPredicateCell2),
            new BinaryBoolean.And<>(cellPredicateCell3, cellPredicateCell4)
    );

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));
    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().iterator().next();

    assertThat(indexedCellRange.operation(), is(GREATER_THAN));
    assertThat(indexedCellRange.start(), is(0));
    assertThat(indexedCellRange.end(), is(15));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("intCell"));
  }


  @Test
  public void testDisruptedUnion() {
    IntrinsicPredicate<Record<String>> cellPredicateCell1 = getCellBasedPredicate("intCell", LESS_THAN, 5);
    IntrinsicPredicate<Record<String>> cellPredicateCell2 = getCellBasedPredicate("intCell", GREATER_THAN, 5);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cellPredicateCell1, cellPredicateCell2);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("intCell"));
    assertThat(bestCellComparison.countIndexRanges(), is(2));
    List<? extends IndexedCellRange<?>> indexedCellRanges = bestCellComparison.indexRanges();

    @SuppressWarnings("rawtypes")
    Set<ComparisonType> operators = indexedCellRanges.stream()
            .peek(range -> assertThat(range.start(), is(5)))
            .peek(range -> assertNull(range.end()))
            .map(IndexedCellRange::operation)
            .collect(Collectors.toSet());
    assertThat(operators, containsInAnyOrder(GREATER_THAN, LESS_THAN));

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_MULTI_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("intCell"));
  }

  @Test
  public void testRepeatedRangeInDisjunction() {
    IntrinsicPredicate<Record<String>> cell1_gt5 = getCellBasedPredicate("cell1", GREATER_THAN, 5);
    IntrinsicPredicate<Record<String>> cell2_eq0 = getCellBasedPredicate("cell2", EQ, 0);
    IntrinsicPredicate<Record<String>> cell2_eq1 = getCellBasedPredicate("cell2", EQ, 1);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(cell1_gt5, cell2_eq0),
            new BinaryBoolean.And<>(cell1_gt5, cell2_eq1)
    );

    @SuppressWarnings("unchecked")
    LocatorGenerator<String> asymmetricIndexLocatorGenerator = Mockito.mock(LocatorGenerator.class);

    when(asymmetricIndexLocatorGenerator.isIndexed(any())).thenReturn(true);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell1", 100L);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell2", 1000L);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            asymmetricIndexLocatorGenerator);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));

    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().iterator().next();
    assertThat(indexedCellRange.operation(), is(GREATER_THAN));
    assertThat(indexedCellRange.start(), is(5));
    assertNull(indexedCellRange.end());

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell1"));
  }

  @Test
  public void testUnionWithUndefinedStart() {
    IntrinsicPredicate<Record<String>> cell1_lt = getCellBasedPredicate("cell1", LESS_THAN, 10);
    IntrinsicPredicate<Record<String>> cell1_gt = getCellBasedPredicate("cell1", GREATER_THAN, 0);
    IntrinsicPredicate<Record<String>> cell2_eq0 = getCellBasedPredicate("cell2", EQ, 0);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(cell1_lt, cell2_eq0),
            new BinaryBoolean.And<>(cell1_lt, cell1_gt)
    );

    @SuppressWarnings("unchecked")
    LocatorGenerator<String> asymmetricIndexLocatorGenerator = Mockito.mock(LocatorGenerator.class);

    when(asymmetricIndexLocatorGenerator.isIndexed(any())).thenReturn(true);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell1", 100L);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell2", 1000L);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            asymmetricIndexLocatorGenerator);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));

    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().iterator().next();
    assertThat(indexedCellRange.operation(), is(LESS_THAN));
    assertThat(indexedCellRange.start(), is(10));
    assertNull(indexedCellRange.end());

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell1"));
  }


  @Test
  public void testUnionWithUndefinedEnd() {
    IntrinsicPredicate<Record<String>> cell1_lt = getCellBasedPredicate("cell1", LESS_THAN, 10);
    IntrinsicPredicate<Record<String>> cell1_gt = getCellBasedPredicate("cell1", GREATER_THAN, 0);
    IntrinsicPredicate<Record<String>> cell2_eq0 = getCellBasedPredicate("cell2", EQ, 0);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(cell1_gt, cell2_eq0),
            new BinaryBoolean.And<>(cell1_lt, cell1_gt)
    );

    @SuppressWarnings("unchecked")
    LocatorGenerator<String> asymmetricIndexLocatorGenerator = Mockito.mock(LocatorGenerator.class);

    when(asymmetricIndexLocatorGenerator.isIndexed(any())).thenReturn(true);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell1", 100L);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell2", 1000L);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            asymmetricIndexLocatorGenerator);

    assertThat(bestCellComparison, is(notNullValue()));
    assertThat(getUniqueFragmentDefinitionName(bestCellComparison), is("cell1"));
    assertThat(bestCellComparison.countIndexRanges(), is(1));

    IndexedCellRange<?> indexedCellRange = bestCellComparison.indexRanges().iterator().next();
    assertThat(indexedCellRange.operation(), is(GREATER_THAN));
    assertThat(indexedCellRange.start(), is(0));
    assertNull(indexedCellRange.end());

    assertThat(tracker.isSortedCellIndexUsed(), is(true));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SORTED_INDEX_SCAN));
    assertThat(getUniqueIndexedCellDefinitionName(executingPlan), is("cell1"));
  }

  /**
   * The union consists of a single range whose start and end are null.
   * Ideally, the entire index should be scanned.
   * However, currently this falls back to a FDS (see TDB-3724).
   */
  @Test
  public void testInfiniteUnion2() {
    IntrinsicPredicate<Record<String>> cell1_lt = getCellBasedPredicate("cell1", LESS_THAN, 10);
    IntrinsicPredicate<Record<String>> cell1_gt = getCellBasedPredicate("cell1", GREATER_THAN, 0);
    IntrinsicPredicate<Record<String>> cell2_eq0 = getCellBasedPredicate("cell2", EQ, 0);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(
            new BinaryBoolean.And<>(cell1_gt, cell2_eq0),
            new BinaryBoolean.And<>(cell1_lt, cell2_eq0)
    );

    @SuppressWarnings("unchecked")
    LocatorGenerator<String> asymmetricIndexLocatorGenerator = Mockito.mock(LocatorGenerator.class);

    when(asymmetricIndexLocatorGenerator.isIndexed(any())).thenReturn(true);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell1", 100L);
    setIndexCoverageEstimate(asymmetricIndexLocatorGenerator, "cell2", 1000L);

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker,
            asymmetricIndexLocatorGenerator);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.FULL_DATASET_SCAN));
  }

  @Test
  public void testAlwaysFalse() {
    IntrinsicPredicate<Record<String>> cell2_eq0 = getCellBasedPredicate("cell2", EQ, 0);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.And<>(cell2_eq0,
            AlwaysTrue.alwaysTrue().negate());

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.SKIP_SCAN));
  }

  @Test
  public void testAlwaysTrue() {
    IntrinsicPredicate<Record<String>> cell2_eq0 = getCellBasedPredicate("cell2", EQ, 0);
    IntrinsicPredicate<Record<String>> expression = new BinaryBoolean.Or<>(cell2_eq0, AlwaysTrue.alwaysTrue());

    StreamPlanTracker tracker = new StreamPlanTracker(Collections.singleton(plan -> {
    }));
    CellComparison<String> bestCellComparison = optimize(expression, tracker);

    assertThat(bestCellComparison, is(nullValue()));
    Plan executingPlan = tracker.executingPlan();
    assertThat(executingPlan.getPlanType(), is(Plan.PlanType.FULL_DATASET_SCAN));
  }

  private String getUniqueIndexedCellDefinitionName(Plan executingPlan) {
    SortedIndexPlan<?> sortedIndexPlan = (SortedIndexPlan<?>) executingPlan;
    return getUniqueDefinitionName(sortedIndexPlan.indexedCellSelection());
  }

  private String getUniqueFragmentDefinitionName(CellComparison<?> bestCellComparison) {
    return getUniqueDefinitionName(bestCellComparison);
  }

  private String getUniqueDefinitionName(IndexedCellSelection bestCellComparison) {
    Set<String> names = getCellDefinitionNames(bestCellComparison);
    assertEquals(1, names.size());
    return names.iterator().next();
  }

  @SuppressWarnings("rawtypes")
  private Set<String> getCellDefinitionNames(IndexedCellSelection bestCellComparison) {
    return bestCellComparison.indexRanges()
              .stream()
              .map(IndexedCellRange::getCellDefinition)
              .map(CellDefinition::name)
              .collect(Collectors.toSet());
  }

  private IntrinsicPredicate<Record<String>> getKeyBasedPredicate() {
    return new NonGatedComparison.Equals<>(new RecordKey<>(), new Constant<>("foo"));
  }

  @SuppressWarnings("unchecked")
  private <K extends Comparable<K>>
  IntrinsicPredicate<Record<K>> getCellBasedPredicate(String cellName, ComparisonType op, int val) {
    IntCellDefinition def = CellDefinition.defineInt(cellName);
    @SuppressWarnings("RedundantCast") IntrinsicFunction<Record<K>, Optional<Integer>> cellExtractor =
            (IntrinsicFunction<Record<K>, Optional<Integer>>) (Object) CellExtractor.extractComparable(def);
    switch (op) {
      case EQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val));
      case NEQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val)).negate();
      default:
        return new GatedComparison.Contrast<>(cellExtractor, op, new Constant<>(val));
    }
  }

  @SuppressWarnings({"unchecked", "RedundantCast"})
  private static <K extends Comparable<K>, V> IntrinsicPredicate<Record<K>> getCellBasedPredicate(
          CellDefinition<V> def, ComparisonType op, V val) {

    IntrinsicFunction<Record<K>, Optional<V>> cellExtractor =
            (IntrinsicFunction<Record<K>, Optional<V>>) (Object) CellExtractor.extractPojo(def);
    switch (op) {
      case EQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val));
      case NEQ:
        return new GatedComparison.Equals<>(cellExtractor, new Constant<>(val)).negate();
      default:
        throw new IllegalArgumentException();
    }
  }
}