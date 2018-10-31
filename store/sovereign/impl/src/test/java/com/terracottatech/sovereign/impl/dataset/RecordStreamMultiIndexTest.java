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
import com.terracottatech.sovereign.plan.StreamPlan;
import com.terracottatech.store.definition.CellDefinition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.AnimalsDataset;
import com.terracottatech.sovereign.plan.Plan;
import com.terracottatech.sovereign.plan.SortedIndexPlan;
import com.terracottatech.store.Record;
import org.omg.CORBA.PRIVATE_MEMBER;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.terracottatech.test.data.Animals.ANIMALS;
import static com.terracottatech.test.data.Animals.NEAR_THREATENED;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static com.terracottatech.test.data.Animals.VULNERABLE;

/**
 * Testing queries containing multiple indexed cells.
 *
 * @author RKAV
 */
public class RecordStreamMultiIndexTest {
  private static final long actualStatusCount =
      ANIMALS
          .stream()
          .filter((a) -> a.getStatus() != null && a.getStatus().compareTo(VULNERABLE) <= 0)
          .filter((a) -> a.getStatus().compareTo(NEAR_THREATENED) >= 0)
          .count();

  private static final long actualAnimalCountForConjunctionFilter =
      ANIMALS
          .stream()
          .filter((a) -> a.getObservations() != null && a.getObservations() > 5L)
          .filter((a) -> a.getStatus() != null && a.getStatus().compareTo(VULNERABLE) <= 0)
          .filter((a) -> a.getStatus().compareTo(NEAR_THREATENED) >= 0)
          .filter((a) -> a.getTaxonomicClass() != null && a.getTaxonomicClass().compareTo("m") > 0)
          .filter((a) -> a.getTaxonomicClass().compareTo("n") < 0)
          .count();

  private static final long actualThreatenedOrVulnerableCount =
      ANIMALS
          .stream()
          .filter((a) -> a.getStatus() != null && (a.getStatus().equals(VULNERABLE) || a.getStatus().equals(NEAR_THREATENED)))
          .count();

  private static final long actualMammalsAndBirdsThreatenedOrVulnerable =
      ANIMALS
          .stream()
          .filter((a) -> (a.getTaxonomicClass() != null && (a.getTaxonomicClass()
                                                                .equals("mammal") || a.getTaxonomicClass()
                                                                .equals("bird"))))
          .filter((a) -> a.getStatus() != null && (a.getStatus().equals(NEAR_THREATENED) || a.getStatus().equals(VULNERABLE)))
          .count();

  private SovereignDataset<String> dataset;

  @Before
  public void setUp() throws Exception {
    this.dataset = AnimalsDataset.createDataset(16 * 1024 * 1024);
    AnimalsDataset.addIndexes(this.dataset);
  }

  @After
  public void tearDown() throws Exception {
    this.dataset.getStorage().destroyDataSet(dataset.getUUID());
  }

  private RecordStream<String> getTestStream() {
    return this.dataset.records();
  }

  @Test
  public void testMultipleIndexedCellInPredicateWithConjunction() {
    Predicate<Record<?>> obs = OBSERVATIONS.value().isGreaterThan(5L);
    Predicate<Record<?>> status =
        STATUS.value().isLessThanOrEqualTo(VULNERABLE)
            .and(STATUS.value().isGreaterThanOrEqualTo(NEAR_THREATENED));
    Predicate<Record<?>> taxonomy =
        TAXONOMIC_CLASS.value().isGreaterThan("m")
            .and(TAXONOMIC_CLASS.value().isLessThan("n"));
    try (RecordStream<String> testStream = getTestStream()) {
      long count = testStream.explain(plan -> {
        Assert.assertEquals(Plan.PlanType.SORTED_INDEX_SCAN, plan.executingPlan().getPlanType());
        // Status is the smaller index and hence it should be selected..
        Assert.assertEquals(STATUS.name(), getIndexedCellDefinitionName(plan));
      }).filter(obs.and(status.and(taxonomy))).count();
      Assert.assertEquals(actualAnimalCountForConjunctionFilter, count);
    }
  }

  @Test
  public void testMultipleIndexedCellWithDisjunction() {
    Predicate<Record<?>> status =
        STATUS.value().is(VULNERABLE)
            .or(STATUS.value().is(NEAR_THREATENED));
    Predicate<Record<?>> taxonomy =
        TAXONOMIC_CLASS.value().is("mammal")
            .or(TAXONOMIC_CLASS.value().is("bird"));
    try (RecordStream<String> testStream = getTestStream()) {
      long count = testStream.explain(plan -> {
        Assert.assertEquals(Plan.PlanType.SORTED_INDEX_MULTI_SCAN, plan.executingPlan().getPlanType());
        // Status is the smaller index and hence it should be selected..
        Assert.assertEquals(STATUS.name(), getIndexedCellDefinitionName(plan));
      }).filter(taxonomy).filter(status).count();
      Assert.assertEquals(actualMammalsAndBirdsThreatenedOrVulnerable, count);
    }
  }

  @Test
  public void testMultipleIndexedCellsWithDisjunctionAndNegation() {
    Predicate<Record<?>> status =
        STATUS.value().isLessThan(NEAR_THREATENED)
            .or(STATUS.value().isGreaterThan(VULNERABLE)).negate();
    Predicate<Record<?>> taxonomy =
        TAXONOMIC_CLASS.value().isGreaterThanOrEqualTo("a")
            .and(TAXONOMIC_CLASS.value().isLessThanOrEqualTo("zzzzzzzzzzzzz"));
    try (RecordStream<String> testStream = getTestStream()) {
      long count = testStream.explain(plan -> {
        Assert.assertEquals(Plan.PlanType.SORTED_INDEX_SCAN, plan.executingPlan().getPlanType());
        // Status is the smaller index and hence it should be selected..
        Assert.assertEquals(STATUS.name(), getIndexedCellDefinitionName(plan));
      }).filter(taxonomy.and(status)).count();
      Assert.assertEquals(actualThreatenedOrVulnerableCount, count);
    }
  }

  private String getIndexedCellDefinitionName(StreamPlan plan) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Set<String> names = ((SortedIndexPlan<String>) plan.executingPlan())
            .indexedCellSelection()
            .indexRanges()
            .stream()
            .map(IndexedCellRange::getCellDefinition)
            .map(CellDefinition::name)
            .collect(Collectors.toSet());
    Assert.assertEquals(1, names.size());
    return names.iterator().next();
  }
}
