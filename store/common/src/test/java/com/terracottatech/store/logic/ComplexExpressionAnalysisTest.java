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

import com.bpodgursky.jbool_expressions.Expression;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.logic.NormalForm.Type.DISJUNCTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test that the repeated analysis of identical complex expressions yields
 * the same results and takes roughly the same time.
 */
public class ComplexExpressionAnalysisTest {

  private static final String STATS_NAMES = "Cache:GetHitLatency,Cache:GetHitLatency#100,Cache:GetHitLatency#50," +
          "Cache:GetHitLatency#95,Cache:GetHitLatency#99,Cache:GetMissLatency,Cache:GetMissLatency#100," +
          "Cache:GetMissLatency#50,Cache:GetMissLatency#95,Cache:GetMissLatency#99,Cache:PutLatency," +
          "Cache:PutLatency#100,Cache:PutLatency#50,Cache:PutLatency#95,Cache:PutLatency#99," +
          "Cache:RemoveLatency,Cache:RemoveLatency#100,Cache:RemoveLatency#50,Cache:RemoveLatency#95," +
          "Cache:RemoveLatency#99,Cache:PutCount,Cache:PutRate,Cache:RemovalCount,Cache:RemovalRate," +
          "Cache:ExpirationCount,Cache:ExpirationRate,Cache:EvictionCount,Cache:EvictionRate,Cache:HitCount," +
          "Cache:HitRate,Cache:HitRatio,Cache:MissCount,Cache:MissRate,Cache:MissRatio,Clustered:RemovalCount," +
          "Clustered:RemovalRate,Clustered:PutCount,Clustered:PutRate,Clustered:ExpirationCount," +
          "Clustered:ExpirationRate,Clustered:EvictionCount,Clustered:EvictionRate,Clustered:HitCount," +
          "Clustered:HitRate,Clustered:HitRatio,Clustered:MissCount,Clustered:MissRate,Clustered:MissRatio," +
          "Disk:RemovalCount,Disk:RemovalRate,Disk:PutCount,Disk:PutRate,Disk:ExpirationCount," +
          "Disk:ExpirationRate,Disk:EvictionCount,Disk:EvictionRate,Disk:HitCount,Disk:HitRate," +
          "Disk:HitRatio,Disk:MissCount,Disk:MissRate,Disk:MissRatio,Disk:MappingCount," +
          "Disk:AllocatedByteSize,Disk:OccupiedByteSize,OffHeap:RemovalCount,OffHeap:RemovalRate," +
          "OffHeap:PutCount,OffHeap:PutRate,OffHeap:ExpirationCount,OffHeap:ExpirationRate," +
          "OffHeap:EvictionCount,OffHeap:EvictionRate,OffHeap:HitCount,OffHeap:HitRate,OffHeap:HitRatio," +
          "OffHeap:MissCount,OffHeap:MissRate,OffHeap:MissRatio,OffHeap:MappingCount,OffHeap:AllocatedByteSize," +
          "OffHeap:OccupiedByteSize,OnHeap:RemovalCount,OnHeap:RemovalRate,OnHeap:PutCount,OnHeap:PutRate," +
          "OnHeap:ExpirationCount,OnHeap:ExpirationRate,OnHeap:EvictionCount,OnHeap:EvictionRate,OnHeap:HitCount," +
          "OnHeap:HitRate,OnHeap:HitRatio,OnHeap:MissCount,OnHeap:MissRate,OnHeap:MissRatio,OnHeap:MappingCount," +
          "OnHeap:OccupiedByteSize";

  private static final LongCellDefinition TIMESTAMP = defineLong("timestamp");
  private static final StringCellDefinition NAME = defineString("name");
  private static final StringCellDefinition TYPE = defineString("type");
  private static final StringCellDefinition CAPABILITY = defineString("capability");

  private static final IntrinsicPredicate<Record<?>> predicate1 = createPredicate();
  private static final IntrinsicPredicate<Record<?>> predicate2 = createPredicate();

  @SuppressWarnings("ConstantConditions")
  private static IntrinsicPredicate<Record<?>> createPredicate() {

    BuildablePredicate<Record<?>> timestampFiler = TIMESTAMP.value()
            .isGreaterThan(0L);

    Predicate<Record<?>> statisticNameFiler = Stream.of(STATS_NAMES.split(","))
            .sorted()
            .map(name -> NAME.value().is(name))
            .reduce(BuildablePredicate::or)
            .get();
    Predicate<Record<?>> typeFilter = Arrays.stream(
            "MULTI_LINE, LATENCY, COUNTER, RATE, RATIO, GAUGE, SIZE".split(", "))
            .sorted()
            .map(type -> TYPE.value().is(type))
            .reduce(BuildablePredicate::or)
            .get();
    Predicate<Record<?>> capabilityFilter = CAPABILITY.value()
            .is("StatisticsCapability");
    Predicate<Record<?>> statisticNameFiler2 = Stream.of("Cache:HitCount", "Cache:MissCount")
            .sorted()
            .map(name -> NAME.value().is(name))
            .reduce(BuildablePredicate::or)
            .get();
    BuildablePredicate<Record<?>> filter = timestampFiler
            .and(statisticNameFiler)
            .and(typeFilter)
            .and(capabilityFilter)
            .and(statisticNameFiler2);
    return (IntrinsicPredicate<Record<?>>) filter;
  }

  @Test
  public void testInputExpressions() {
    assertEquals(predicate1, predicate2);
    Expression<IntrinsicWrapper<Record<?>>> expression1 = new BooleanExpressionBuilder<>(predicate1)
            .build();
    Expression<IntrinsicWrapper<Record<?>>> expression2 = new BooleanExpressionBuilder<>(predicate2)
            .build();
    assertEquals(expression1.toString(), expression2.toString());
  }

  @Test
  public void testResults() {
    BooleanAnalyzer<Record<?>> analyzer = new BooleanAnalyzer<>();
    NormalForm<Record<?>, IntrinsicPredicate<Record<?>>> result1 = analyzer.toNormalForm(predicate1, DISJUNCTIVE);
    NormalForm<Record<?>, IntrinsicPredicate<Record<?>>> result2 = analyzer.toNormalForm(predicate2, DISJUNCTIVE);
    assertEquals(result1, result2);
  }

  @Ignore("This is an ad-hoc non-regression test.")
  @Test
  public void testTimes() {
    BooleanAnalyzer<Record<?>> analyzer = new BooleanAnalyzer<>();
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

    long time1 = 0;
    long time2 = 0;

    for (int i = 0; i < 1e3; i++) {
      long start = mxBean.getCurrentThreadCpuTime();
      analyzer.toNormalForm(predicate1, DISJUNCTIVE);
      time1 += mxBean.getCurrentThreadCpuTime() - start;

      start = mxBean.getCurrentThreadCpuTime();
      analyzer.toNormalForm(predicate2, DISJUNCTIVE);
      time2 += mxBean.getCurrentThreadCpuTime() - start;
    }
    assertRatioWithinFactorOfTwo(time1, time2);
  }

  private void assertRatioWithinFactorOfTwo(double time1, double time2) {
    final double lb = 0.5;
    final double ub = 2.0;
    assertRatioWithin(time1, time2, lb, ub);
  }

  private void assertRatioWithin(double time1, double time2, double lb, double ub) {
    double ratio = time1 / time2;
    assertThat(ratio, Matchers.greaterThan(lb));
    assertThat(ratio, Matchers.lessThan(ub));
  }

  /**
   * Test that the result is insensitive to
   * redundant predicates in an input expression.
   */
  @Test
  public void testResultWithAndWithoutRepeatingPredicates() {
    IntrinsicPredicate<Record<?>> predicateWithRepeats = createPredicateWithRepeats();
    BooleanAnalyzer<Record<?>> analyzer = new BooleanAnalyzer<>();
    NormalForm<Record<?>, IntrinsicPredicate<Record<?>>> dnf1 = analyzer
            .toNormalForm(predicate1, DISJUNCTIVE);
    NormalForm<Record<?>, IntrinsicPredicate<Record<?>>> dnf2 = analyzer
            .toNormalForm(predicateWithRepeats, DISJUNCTIVE);
    assertEquals(dnf1, dnf2);
  }

  /**
   * Test that the performance is insensitive to
   * redundant predicates in an input expression.
   */
  @Ignore("This is an ad-hoc non-regression test.")
  @Test
  public void testTimesWithAndWithoutRepeatingPredicates() {
    IntrinsicPredicate<Record<?>> predicateWithRepeats = createPredicateWithRepeats();
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
    BooleanAnalyzer<Record<?>> analyzer = new BooleanAnalyzer<>();

    long time1 = 0;
    long time2 = 0;

    for (int i = 0; i < 1e3; i++) {
      long start = mxBean.getCurrentThreadCpuTime();
      analyzer.toNormalForm(predicate1, DISJUNCTIVE);
      time1 += mxBean.getCurrentThreadCpuTime() - start;

      start = mxBean.getCurrentThreadCpuTime();
      analyzer.toNormalForm(predicateWithRepeats, DISJUNCTIVE);
      time2 += mxBean.getCurrentThreadCpuTime() - start;
    }
    assertRatioWithin(time1, time2, 0.25, 1);
  }

  @SuppressWarnings("ConstantConditions")
  private IntrinsicPredicate<Record<?>> createPredicateWithRepeats() {
    BuildablePredicate<Record<?>> repeats = Stream.generate(() -> TIMESTAMP.value().isGreaterThan(0L))
            .limit(1000)
            .reduce(BuildablePredicate::or)
            .get();
    return (IntrinsicPredicate<Record<?>>) predicate1.and(repeats);
  }

  @Ignore("This is an ad-hoc benchmark test.")
  @Test
  public void testPerformance() {
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
    BooleanAnalyzer<Record<?>> analyzer = new BooleanAnalyzer<>();
    long total = 0;
    for (int i = 0; i < 10; i++) {
      long start = mxBean.getCurrentThreadCpuTime();
      analyzer.toNormalForm(predicate1, DISJUNCTIVE);
      long time = mxBean.getCurrentThreadCpuTime() - start;
      System.out.println(time);
      total += time;
    }
    System.out.println("Total: " + total);
  }
}
