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

import com.terracottatech.sovereign.plan.IndexedCellRange;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.AnimalsDataset;
import com.terracottatech.sovereign.plan.Plan.PlanType;
import com.terracottatech.sovereign.plan.SortedIndexPlan;
import com.terracottatech.store.Record;

import javax.annotation.MatchesPattern;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.terracottatech.test.data.Animals.ANIMALS;
import static com.terracottatech.test.data.Animals.CRITICALLY_ENDANGERED;
import static com.terracottatech.test.data.Animals.EXTINCT;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;

/**
 *
 * @author RKAV
 */
public class RecordStreamExplainTest {
  private static final long actualAnimalCount = ANIMALS.size();
  private static final long actualObservationsGt100Count = ANIMALS.stream().filter((a) -> a.getObservations() != null && a.getObservations() > 100).count();
  private static final long actualExtinctCount =  ANIMALS.stream().filter((a) -> a.getStatus() != null && a.getStatus().equals(EXTINCT)).count();
  private static final long actualCriticallyEndangeredCount =  ANIMALS.stream().filter((a) -> a.getStatus() != null && a.getStatus().equals(CRITICALLY_ENDANGERED)).count();
  private static final long actualMammalsAndBirdsWithObservationsBtwn5And1000 =
      ANIMALS.stream().filter((a) -> (a.getTaxonomicClass() != null && (a.getTaxonomicClass().equals("mammal") || a.getTaxonomicClass().equals("bird")))
                                     && (a.getObservations() != null && a.getObservations() > 5L && a.getObservations() < 1000L)).count();
  private static final long actualMammalsAndBirds =
      ANIMALS.stream().filter((a) -> (a.getTaxonomicClass() != null &&
                                      (a.getTaxonomicClass().equals("mammal") || a.getTaxonomicClass().equals("bird")))).count();

  private SovereignDataset<String> dataset;

  @Before
  public void setUp() throws Exception {
    this.dataset = AnimalsDataset.createDataset(16 * 1024 * 1024);
    AnimalsDataset.addIndexes(this.dataset);
  }

  @After
  public void tearDown() throws Exception {
    dataset.getStorage().destroyDataSet(dataset.getUUID());
  }

  private RecordStream<String> getTestStream() {
    return this.dataset.records();
  }

  @Test
  public void testZeroPredicatesExplain() {
    getTestStream().explain(plan -> {
      Assert.assertEquals("true", plan.usedFilterExpression());
    }).count();
  }

  @Test
  public void testWithTransparentPredicates() {
    Predicate<Record<?>> gte = OBSERVATIONS.value().isGreaterThan(100L);
    long count = ((RecordStream<String>)getTestStream().filter(gte)).explain(plan -> {
      Assert.assertEquals(0, plan.unusedKnownFilterCount());
    }).count();
    Assert.assertEquals(actualObservationsGt100Count, count);
  }

  @Test
  public void testWithTransparentPredicatesWithIndex() {
    Predicate<Record<?>> status = STATUS.value().is(EXTINCT).or(STATUS.value().is(CRITICALLY_ENDANGERED));
    long count = ((RecordStream<String>)getTestStream().filter(status)).explain(plan -> {
      String expectedInput = "((status==extinct)||(status==critically endangered))";
      Assert.assertEquals(expectedInput, plan.usedFilterExpression());
      String reversedInput = "((status==critically endangered)||(status==extinct))";
      Assert.assertThat(plan.getNormalizedFilterExpression().get(), anyOf(is(expectedInput), is(reversedInput)));
      Assert.assertEquals(0, plan.unusedKnownFilterCount());
    }).count();
    long actual = actualCriticallyEndangeredCount + actualExtinctCount;
    Assert.assertEquals(actual, count);
  }

  @Test
  public void testWithOpaquePredicates() {
    Predicate<Record<?>> status = (r) -> (r).get(STATUS).orElse("NOTEXTINCT").equals(EXTINCT);
    long count = ((RecordStream<String>)getTestStream().filter(status)).explain(plan -> {
      Assert.assertEquals(1, plan.unknownFilterCount());
      Assert.assertEquals(0, plan.unusedKnownFilterCount());
    }).count();
    Assert.assertEquals(actualExtinctCount, count);
  }

  @Test
  public void testWithMixedPredicates() {
    Predicate<Record<?>> obs = OBSERVATIONS.value().isGreaterThan(5L);
    Predicate<Record<?>> status1 = (r) -> (r).get(OBSERVATIONS).orElse(100000L) < 1000L;
    Predicate<Record<?>> status2 = TAXONOMIC_CLASS.value().is("mammal").or(TAXONOMIC_CLASS.value().is("bird"));
    long count = ((RecordStream<String>)getTestStream().filter(status2).filter(status1).filter(obs)).explain(plan -> {
      Assert.assertEquals(1, plan.unknownFilterCount());
      Assert.assertEquals(1, plan.unusedKnownFilterCount());
      Assert.assertTrue(plan.executingPlan().getPlanType() == PlanType.SORTED_INDEX_MULTI_SCAN);
    }).count();
    Assert.assertEquals(actualMammalsAndBirdsWithObservationsBtwn5And1000, count);
  }

  @Test
  public void testWithMixedPredicatesMultiRange() {
    Predicate<Record<?>> obs = OBSERVATIONS.value().isGreaterThan(5L);
    Predicate<Record<?>> status1 = (r) -> (r).get(OBSERVATIONS).orElse(100000L) < 1000L;
    Predicate<Record<?>> status2 = (TAXONOMIC_CLASS.value().isGreaterThan("m").and(TAXONOMIC_CLASS.value().isLessThan("n")))
        .or(TAXONOMIC_CLASS.value().is("bird"));
    long count = ((RecordStream<String>)getTestStream().filter(status2).filter(status1).filter(obs)).explain(plan -> {
      Assert.assertEquals(1, plan.unknownFilterCount());
      Assert.assertEquals(1, plan.unusedKnownFilterCount());
      Assert.assertThat(plan.executingPlan().getSubPlans(), empty());
    }).count();
    Assert.assertEquals(actualMammalsAndBirdsWithObservationsBtwn5And1000, count);
  }

  @Test
  public void testObserveWithMixedPredicatesMultiRange() {
    Predicate<Record<?>> obs = OBSERVATIONS.value().isGreaterThan(5L);
    Predicate<Record<?>> obs1 = (r) -> (r).get(OBSERVATIONS).orElse(100000L) < 1000L;
    Predicate<Record<?>> status2 = (TAXONOMIC_CLASS.value().isGreaterThan("m").and(TAXONOMIC_CLASS.value().isLessThan("n")))
        .or(TAXONOMIC_CLASS.value().is("bird"));
    try (RecordStream<String> testStream = getTestStream()) {
      long count = testStream.explain(plan -> {
        Assert.assertEquals(1, plan.unknownFilterCount());
        Assert.assertEquals(1, plan.unusedKnownFilterCount());
        Assert.assertThat(plan.executingPlan().getSubPlans(), empty());
      }).filter(status2).filter(obs1).filter(obs).count();
    }
  }

  @Test
  public void testObserveWithTransparentPredicatesOverlappingMultiRange() {
    Predicate<Record<?>> obs = OBSERVATIONS.value().isGreaterThan(5L);
    Predicate<Record<?>> obs1 = OBSERVATIONS.value().isLessThan(1000L);
    Predicate<Record<?>> status1 = TAXONOMIC_CLASS.value().isLessThan("n");
    Predicate<Record<?>> status2 = (TAXONOMIC_CLASS.value().isGreaterThan("m").and(TAXONOMIC_CLASS.value().isLessThan("q")))
        .or(TAXONOMIC_CLASS.value().is("bird"));
    try (RecordStream<String> testStream = getTestStream()) {
      long count = testStream.explain(plan -> {
        // check if the end() of range is adjusted
        @SuppressWarnings({"unchecked", "rawtypes"})
        Set<?> ends = ((SortedIndexPlan<String>) plan.executingPlan())
                .indexedCellSelection()
                .indexRanges()
                .stream()
                .map(IndexedCellRange::end)
                .collect(Collectors.toSet());
        Assert.assertThat(ends, Matchers.containsInAnyOrder("bird", "n"));
      }).filter(status2).filter(status1).filter(obs).filter(obs1).count();
      Assert.assertEquals(actualMammalsAndBirdsWithObservationsBtwn5And1000, count);
    }
  }

  @Test
  public void testWithNestedBooleanOperators() {
    IntrinsicPredicate<Record<?>> a = (IntrinsicPredicate<Record<?>>) OBSERVATIONS.value().isGreaterThan(1L);
    IntrinsicPredicate<Record<?>> b = (IntrinsicPredicate<Record<?>>) TAXONOMIC_CLASS.value().is("bird");
    IntrinsicPredicate<Record<?>> c = (IntrinsicPredicate<Record<?>>) STATUS.value().is("extinct");
    Predicate<Record<?>> and = new BinaryBoolean.And<>(a, new BinaryBoolean.And<>(b, c));
    try (RecordStream<String> testStream = getTestStream()) {
      long count = testStream.explain(plan -> {
        Assert.assertEquals("((observations>1)&&((class==bird)&&(status==extinct)))",
                plan.usedFilterExpression());

        String normalized = plan.getNormalizedFilterExpression().get();
        Assert.assertTrue(normalized.matches("\\(\\([a-z>=0-9]+ && [a-z>=0-9]+ && [a-z>=0-9]+\\)\\)"));
        String[] terms = normalized.replaceAll("^\\(\\(", "")
                .replaceAll("\\)\\)$", "")
                .split("\\s+&&\\s+");
        Assert.assertThat(terms, arrayContainingInAnyOrder("observations>1", "class==bird", "status==extinct"));

        Assert.assertTrue(plan.isSortedCellIndexUsed());
        Assert.assertEquals(0, plan.unknownFilterCount());
        Assert.assertEquals(0, plan.unusedKnownFilterCount());
        Assert.assertThat(plan.executingPlan().getSubPlans(), empty());
      }).filter(and).count();
      Assert.assertEquals(0L, count);
    }
  }
}
