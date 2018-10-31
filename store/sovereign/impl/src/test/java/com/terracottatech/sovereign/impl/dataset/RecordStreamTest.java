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

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.spi.store.Locator;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.impl.CellExtractor;
import com.terracottatech.store.logic.Combinatorics;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.store.function.Collectors.counting;
import static com.terracottatech.store.function.Collectors.filtering;
import static com.terracottatech.store.function.Collectors.groupingBy;
import static com.terracottatech.store.function.Collectors.groupingByConcurrent;
import static com.terracottatech.store.function.Collectors.mapping;
import static com.terracottatech.store.function.Collectors.partitioningBy;
import static com.terracottatech.store.function.Collectors.summarizingInt;
import static com.terracottatech.store.intrinsics.impl.CellExtractor.extractComparable;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RecordStreamTest {

  @Mock private SovereignContainer<Integer> data;
  @Mock private SovereignSortedIndexMap<Integer, Integer> map;
  @Mock private Catalog<Integer> c;

  private IntCellDefinition def = CellDefinition.defineInt("test");
  private IntCellDefinition def1 = CellDefinition.defineInt("test1");
  private IntCellDefinition def2 = CellDefinition.defineInt("test2");

  public RecordStreamTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    int limit = 1000;
    when(data.get(any(PersistentMemoryLocator.class))).then((InvocationOnMock top) -> {
      Record<?> r = mock(VersionedRecord.class);
      int index = ((MockLocator) top.getArguments()[0]).getIndex();
      when(r.get(ArgumentMatchers.eq(def))).thenReturn(of(index));
      when(r.get(ArgumentMatchers.eq(def1))).thenReturn(empty());
      when(r.get(ArgumentMatchers.eq(def2))).thenReturn(of(index));
      when(r.spliterator()).thenReturn((Spliterator) Arrays.asList(def.newCell(1), def.newCell(2)).spliterator());
      return r;
    });
    when(c.getContainer()).thenReturn((SovereignContainer)data);
    when(this.data.start(true)).thenReturn(mock(ContextImpl.class));
    when(map.higher(any(ContextImpl.class), any(Integer.class))).then((InvocationOnMock invocation) -> new MockLocator(
      Locator.TraversalDirection.FORWARD, (int)invocation.getArguments()[1]+1,limit));
    when(map.get(any(ContextImpl.class), any(Integer.class))).then((InvocationOnMock invocation) -> new
      MockLocator(Locator.TraversalDirection.FORWARD, (int)invocation.getArguments()[1],limit));
    when(map.higherEqual(any(ContextImpl.class), any(Integer.class))).then((InvocationOnMock invocation) -> new
      MockLocator(Locator.TraversalDirection.FORWARD, (int)invocation.getArguments()[1],limit));
    when(map.lower(any(ContextImpl.class), any(Integer.class))).then((InvocationOnMock invocation) -> new MockLocator(
      Locator.TraversalDirection.REVERSE, (int)invocation.getArguments()[1]-1,limit));
    when(map.lowerEqual(any(ContextImpl.class), any(Integer.class))).then((InvocationOnMock invocation) -> new
      MockLocator(Locator.TraversalDirection.REVERSE, (int)invocation.getArguments()[1],limit));
    when(map.get(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.FORWARD,
                                                       (int) invocation.getArguments()[1], limit));
    when(map.higherEqual(any(ContextImpl.class), any(Integer.class))).then(
      (InvocationOnMock invocation) -> new MockLocator(Locator.TraversalDirection.FORWARD,
                                                       (int) invocation.getArguments()[1], limit));
    when(c.getContainer()).thenReturn((SovereignContainer)data);
    when(c.hasSortedIndex(def)).thenReturn(Boolean.TRUE);
    when(c.getSortedIndexFor(def)).thenReturn(map);
    when(c.hasSortedIndex(def2)).thenReturn(Boolean.TRUE);
    when(c.getSortedIndexFor(def2)).thenReturn(map);
    when(this.data.first(any(ContextImpl.class))).thenReturn(new MockLocator(Locator.TraversalDirection.FORWARD, 0,
      1000));
  }

  private RecordStream<Integer> getRecordStream() {
    return RecordStreamImpl.newInstance(c, false);
  }

  @After
  public void tearDown() {
  }

  @SuppressWarnings("SimplifiableConditionalExpression")
  @Test
  public void testComboPredicate() {
    Predicate<Record<?>> first = extractComparable(def).isGreaterThan(5);

    Predicate<Record<?>> second = extractComparable(def).isLessThan(10);

    Predicate<Record<?>> combo = first.and(second);

    Assert.assertTrue(first instanceof Intrinsic);
    Assert.assertTrue(second instanceof Intrinsic);
    Assert.assertTrue(combo instanceof Intrinsic);
  }

  @Test
  public void testBracketPredicate() {
    Predicate<Record<?>> lt = extractComparable(def).isLessThan(100);
    Predicate<Record<?>> gt = extractComparable(def).isGreaterThanOrEqualTo(91);
    long count = getRecordStream().filter(lt).filter(gt).count();
    Assert.assertEquals(9L, count);
    verify(data, times(10)).get(ArgumentMatchers.any());
  }

  @Test
  public void testOverlapPredicate() {
    Predicate<Record<?>> lt = def.value().isLessThan(100);
    Predicate<Record<?>> lt2 = def.value().isLessThan(50);
    long count = getRecordStream().filter(lt).filter(lt2).count();
    Assert.assertEquals(50L, count);
    verify(data, times(50)).get(ArgumentMatchers.any());
    Assert.assertEquals(50L, getRecordStream().filter(lt2).filter(lt).count());
    verify(data, times(100)).get(ArgumentMatchers.any());
  }

  @Test
  public void testBracketConjunctionPredicate() {
    Predicate<Record<?>> lt = def.value().isLessThan(100);
    Predicate<Record<?>> gt = def.value().isGreaterThanOrEqualTo(91);
    long count = getRecordStream().filter(lt.and(gt)).count();
    Assert.assertEquals(9L, count);
    verify(data, times(10)).get(ArgumentMatchers.any());
  }

  @Test
  public void testOverlapConjunctionPredicateLessThan() {
    Predicate<Record<?>> lt = def.value().isLessThan(100);
    Predicate<Record<?>> lt2 = def.value().isLessThan(49);
    long count = getRecordStream().filter(lt.and(lt2)).count();
    Assert.assertEquals(49L, count);
    verify(data, times(49)).get(ArgumentMatchers.any());
  }

  @Test
  public void testOverlapConjunctionPredicateGreaterThan() {
    Predicate<Record<?>> lt = def.value().isGreaterThan(500);
    Predicate<Record<?>> lt2 = def.value().isGreaterThan(2);
    long count = getRecordStream().filter(lt.and(lt2)).count();
    Assert.assertEquals(500L, count);
    verify(data, times(500)).get(ArgumentMatchers.any());
  }

  @Test
  public void testRangeConjunctionPredicate() {
    Predicate<Record<?>> lte = def.value().isLessThanOrEqualTo(500);
    Predicate<Record<?>> gt = def.value().isGreaterThan(100);
    long count = getRecordStream().filter(lte.and(gt)).count();
    Assert.assertEquals(400L, count);
    verify(data, times(401)).get(ArgumentMatchers.any());
  }

  @Test
  public void testRangeConjunctionPredicateReverse() {
    Predicate<Record<?>> lt = def.value().isLessThan(500);
    Predicate<Record<?>> gte = def.value().isGreaterThanOrEqualTo(100);
    long count = getRecordStream().filter(gte.and(lt)).count();
    Assert.assertEquals(400L, count);
    verify(data, times(401)).get(ArgumentMatchers.any());
  }

  @Test
  public void testRangeConjunctionPredicateWithOverlap() {
    Predicate<Record<?>> lt1 = def.value().isLessThan(501);
    Predicate<Record<?>> lt2 = def.value().isLessThan(5000);
    Predicate<Record<?>> lte1 = def.value().isLessThanOrEqualTo(500);
    Predicate<Record<?>> lte2 = def.value().isLessThanOrEqualTo(999);
    Predicate<Record<?>> gt1 = def.value().isGreaterThan(100);
    Predicate<Record<?>> gt2 = def.value().isGreaterThan(10);
    Predicate<Record<?>> gte1 = def.value().isGreaterThanOrEqualTo(100);
    Predicate<Record<?>> gte2 = def.value().isGreaterThanOrEqualTo(1);
    long count = getRecordStream().filter(lt1.and(lt2.and(lte1).and(gt1).and(gt2)).and(gte1).and(gte2).and(lte2)).count();
    Assert.assertEquals(400L, count);
    verify(data, times(401)).get(ArgumentMatchers.any());
  }

  /**
   * (t <= 500) & (t > 100)  & (t >= 100)
   */
  @Test
  public void testOrderingsForLowerBound() {
    Predicate<Record<?>> lte500 = def.value().isLessThanOrEqualTo(500);
    Predicate<Record<?>> gt100 = def.value().isGreaterThan(100);
    Predicate<Record<?>> gte100 = def.value().isGreaterThanOrEqualTo(100);
    Combinatorics.allOrderings(lte500, gt100, gte100)
            .map(this::toConjunction)
            .map(and -> getRecordStream().filter(and))
            .map(Stream::count)
            .forEach(count -> Assert.assertEquals(400L, (long) count));
  }

  /**
   * (t > 100) & (t < 500) & (t <= 500)
   */
  @Test
  public void testOrderingsForUpperBound() {
    Predicate<Record<?>> gt100 = def.value().isGreaterThan(100);
    Predicate<Record<?>> lt500 = def.value().isLessThan(500);
    Predicate<Record<?>> lte500 = def.value().isLessThanOrEqualTo(500);
    Combinatorics.allOrderings(gt100, lt500, lte500)
            .map(this::toConjunction)
            .map(and -> getRecordStream().filter(and))
            .map(Stream::count)
            .forEach(count -> Assert.assertEquals(399L, (long) count));
  }

  private Predicate<Record<?>> toConjunction(Predicate<Record<?>>[] predicates) {
    Predicate<Record<?>> first = predicates[0];
    return Stream.of(predicates)
            .skip(1)
            .reduce(first, Predicate::and);
  }

  @Test
  public void testDisjunctions() {
    Predicate<Record<?>> lt1 = def.value().isLessThanOrEqualTo(500);
    Predicate<Record<?>> gte2 = def.value().isGreaterThanOrEqualTo(900);
    long count = getRecordStream().filter(lt1.or(gte2)).count();
    Assert.assertEquals(602L, count);
    verify(data, times(602)).get(ArgumentMatchers.any());
  }

  @Ignore("Until the consistency of multi-index scan is guaranteed.")
  @Test
  public void testDisjunctionOnDifferentIndexedCells() {
    Predicate<Record<?>> range = def.value().isGreaterThanOrEqualTo(1).and(def.value().isLessThan(3));
    Predicate<Record<?>> range2 = def2.value().isGreaterThanOrEqualTo(3).and(def2.value().isLessThan(5));
    long count = getRecordStream().filter(range.or(range2)).count();
    Assert.assertEquals(4L, count);
    verify(data, times(6)).get(ArgumentMatchers.any());
  }

  @Test
  public void testLogicalContradiction() {
    Predicate<Record<?>> range = def.value().isGreaterThanOrEqualTo(1).and(def.value().isLessThan(3));
    long count = getRecordStream().filter(range.and(range.negate())).count();
    Assert.assertEquals(0L, count);
    verify(data, never()).get(ArgumentMatchers.any());
  }

  @Test
  public void testIndexedCellPredicateOrLogicalContradiction() {
    Predicate<Record<?>> range = def.value().isLessThan(1);
    Predicate<Record<?>> range2 = def2.value().is(3);
    Predicate<Record<?>> filter = range.or(range2.and(range2.negate()));
    long count = getRecordStream().filter(filter).count();
    Assert.assertEquals(1L, count);
    verify(data, times(1)).get(ArgumentMatchers.any());
  }

  /**
   * Note: The range (0, 5) in def index is scanned twice - this may be improved in future.
   * See also {@code com.terracottatech.sovereign.impl.dataset.SovereignOptimizerTest#testCommonIndexedCellInClauses()}
   */
  @Test
  public void testCommonIndexedCellInClauses() {
    Predicate<Record<?>> gt = def.value().isGreaterThan(0);
    Predicate<Record<?>> lt = def.value().isLessThan(5);
    Predicate<Record<?>> exists1 = def1.exists();
    Predicate<Record<?>> lt2 = def2.value().isLessThan(100);
    Predicate<Record<?>> filter = gt.and(exists1).or(lt.and(lt2));
    long count = getRecordStream().filter(filter).count();
    Assert.assertEquals(5L, count);
    verify(data, times(1001)).get(ArgumentMatchers.any());
  }

  @Test
  public void testConjunctionWithDisjunctionsNonExistentCell() {
    Predicate<Record<?>> lt1 = def.value().isLessThan(501);
    Predicate<Record<?>> gte1 = def.value().isGreaterThan(901);
    Predicate<Record<?>> lt21 = CellExtractor.extractComparable(def1).isLessThan(501);
    Predicate<Record<?>> gte22 = CellExtractor.extractComparable(def1).isGreaterThan(901);

    long count = getRecordStream().filter(lt21.and(lt1.and(gte1).and(lt1)).and(gte22.or(lt21))).count();
    Assert.assertEquals(0L, count);
    verify(data, never()).get(ArgumentMatchers.any());
  }

  /**
   * Test a mixture of conjunction and disjunction.
   *
   * Note: All of these uses index selection, which is asserted by verifying how many times
   * data container is traversed.
   */
  @Test
  public void testConjunctionWithDisjunction() {
    final Predicate<Record<?>> gt501 = def.value().isGreaterThan(501);
    final Predicate<Record<?>> lt901 = def.value().isLessThan(901);
    final Predicate<Record<?>> lt101 = def.value().isLessThan(101);

    final long count1 = getRecordStream().filter((gt501.and(lt901.or(lt101)))).count();
    verify(data, times(400)).get(ArgumentMatchers.any());

    final long count2 = getRecordStream().filter((lt901.and(gt501.or(lt101)))).count();
    verify(data, times(901)).get(ArgumentMatchers.any());

    final long count3 = getRecordStream().filter((lt101.and(lt901.or(gt501)))).count();
    verify(data, times(1002)).get(ArgumentMatchers.any());

    Assert.assertEquals(399L, count1);
    Assert.assertEquals(500L, count2);
    Assert.assertEquals(101L, count3);
  }

  /**
   * Test a mixture of conjunctions, disjunctions and negations
   */
  @Test
  public void testConjunctionWithDisjunctionAndNegate() {
    final Predicate<Record<?>> gt100 = def.value().isGreaterThan(100);
    final Predicate<Record<?>> lt200 = def.value().isLessThan(200);
    final Predicate<Record<?>> gte150 = def.value().isGreaterThanOrEqualTo(150);

    final Predicate<Record<?>> lte100 = gt100.negate();
    final Predicate<Record<?>> gte200 = lt200.negate();

    final long count1 = getRecordStream().filter((lte100.and(gte200.or(gte150)))).count();
    verify(data, never()).get(ArgumentMatchers.any());

    final long count2 = getRecordStream().filter((gt100.or(lt200)).negate()).count();
    verify(data, never()).get(ArgumentMatchers.any());

    Assert.assertEquals(0L, count1);
    Assert.assertEquals(0L, count2);
  }

  /**
   * Test a disjunction of overlapping ranges.
   */
  @Test
  public void testOverlappingRanges() {
    final Predicate<Record<?>> gt100 = def.value().isGreaterThan(101);
    final Predicate<Record<?>> lte102 = def.value().isLessThanOrEqualTo(102);
    final Predicate<Record<?>> gte102 = def.value().isGreaterThanOrEqualTo(102);
    final Predicate<Record<?>> lt103 = def.value().isLessThan(103);
    Predicate<Record<?>> range1 = gt100.and(lte102);
    Predicate<Record<?>> range2 = gte102.and(lt103);
    Predicate<Record<?>> overlappingRanges = range1.or(range2);

    long count = getRecordStream()
            .filter(overlappingRanges)
            .count();
    Assert.assertEquals(1L, count);
  }

  /**
   * Test a mixture of conjunctions and disjunctions
   *
   * e.g A AND (B OR (C AND D))
   */
  @Test
  public void testElaborateConjunctionWithDisjunction() {
    final Predicate<Record<?>> a = def.value().isGreaterThanOrEqualTo(100);
    final Predicate<Record<?>> b = def.value().isLessThan(200);
    final Predicate<Record<?>> c = def.value().isGreaterThan(750);
    final Predicate<Record<?>> d = def.value()
        .isLessThanOrEqualTo(1000);

    final long count = getRecordStream().filter(a.and(b.or(c.and(d)))).count();
    verify(data, times(351)).get(ArgumentMatchers.any());

    Assert.assertEquals(350L, count);
  }

  /**
   * Test a mixture of conjunctions and disjunctions
   *
   * e.g A AND (B OR (C AND D))
   */
  @Test
  public void testElaborateConjunctionWithDisjunctionMixedCells() {
    final Predicate<Record<?>> a = def1.value().isGreaterThanOrEqualTo(5);
    final Predicate<Record<?>> b = def.value().isLessThan(200);
    final Predicate<Record<?>> c = def.value().isGreaterThan(750);
    final Predicate<Record<?>> d = def.value().isLessThanOrEqualTo(1000);

    final long count = getRecordStream().filter(a.and(b.or(c.and(d)))).count();
    verify(data, times(450)).get(ArgumentMatchers.any());

    Assert.assertEquals(0L, count);
  }

  @Test
  public void testElaborateConjunctionWithDisjunction2() {
    final Predicate<Record<?>> a = def.value().isGreaterThanOrEqualTo(100);
    final Predicate<Record<?>> b = def.value().isLessThan(200);
    final Predicate<Record<?>> c = def.value().isGreaterThan(750);
    final Predicate<Record<?>> d = def.value().isLessThanOrEqualTo(1000);

    final Predicate<Record<?>> e = def.value().is(400);
    final Predicate<Record<?>> f = def.value().is(500);

    final long count1 = getRecordStream().filter((a.and(b.or(c.and(d)))).or(e.or(f))).count();
    verify(data, times(355)).get(ArgumentMatchers.any());

    final long count2 = getRecordStream().filter((a.or(e.or(f))).and(((b.or(c)).or(e.or(f))).and(((b.or(d)).or(e.or(f)))))).count();
    verify(data, times(710)).get(ArgumentMatchers.any());

    Assert.assertEquals(352L, count1);
    Assert.assertEquals(352L, count2);
  }

  @Test
  public void testElaborateConjunctionWithDisjunction3() {
    final Predicate<Record<?>> a = def.value().isGreaterThanOrEqualTo(100);
    final Predicate<Record<?>> b = def.value().isLessThan(200);
    final Predicate<Record<?>> c = def.value().isGreaterThan(750);
    final Predicate<Record<?>> d = def.value().isLessThanOrEqualTo(1000);

    final Predicate<Record<?>> e = def.value().is(400);
    final Predicate<Record<?>> f = def.value().is(500);

    final long count1 = getRecordStream().filter((a.and(b.or(c.and(d)))).or(e.or(f))).count();
    verify(data, times(355)).get(ArgumentMatchers.any());

    final long count2 = getRecordStream().filter((a.or(e.or(f))).and(((b.or(c)).or(e.or(f))).and(((b.or(d)).or(e.or(f)))))).count();
    verify(data, times(710)).get(ArgumentMatchers.any());

    final long count3 = getRecordStream().filter((b.and(a.or(d.and(c)))).or(f.or(e))).count();
    verify(data, times(815)).get(ArgumentMatchers.any());

    final long count4 = getRecordStream().filter((e.and(f).or(a).or(b).and(c).or(d))).count();
    verify(data, times(1816)).get(ArgumentMatchers.any());

    final Predicate<Record<?>> saved = ((b.or(c)).or(e.or(f))).and((a.or(e.or(f))).and(((b.or(d)).or(e.or(f)))));

    final long count5 = getRecordStream().filter(saved).count();
    verify(data, times(2171)).get(ArgumentMatchers.any());

    final long count6 = getRecordStream().filter(saved.and(e)).count();
    verify(data, times(2173)).get(ArgumentMatchers.any());

    final long count7 = getRecordStream().filter(saved.and(e.or(f))).count();
    verify(data, times(2177)).get(ArgumentMatchers.any());

    final long count8 = getRecordStream().filter(saved.and(e.and(f))).count();
    verify(data, times(2177)).get(ArgumentMatchers.any());

    final long count9 = getRecordStream().filter(saved.and(a.and(d))).count();
    verify(data, times(2532)).get(ArgumentMatchers.any());

    final long count10 = getRecordStream().filter(saved.and(a.and(c))).count();
    verify(data, times(2782)).get(ArgumentMatchers.any());

    Assert.assertEquals(352L, count1);
    Assert.assertEquals(352L, count2);
    Assert.assertEquals(102L, count3);
    Assert.assertEquals(1001L, count4);
    Assert.assertEquals(352L, count5);
    Assert.assertEquals(1L, count6);
    Assert.assertEquals(2L, count7);
    Assert.assertEquals(0L, count8);
    Assert.assertEquals(352L, count9);
    Assert.assertEquals(250L, count10);
  }

  /**
   * Test a mixture of conjunctions, disjunctions and negations, 2nd case
   */
  @Test
  public void testConjunctionWithDisjunctionAndNegate2() {
    final Predicate<Record<?>> gt100 = def.value().isGreaterThan(100);

    final Predicate<Record<?>> gte50 = def.value().isLessThan(50).negate();
    final Predicate<Record<?>> lt75 = def.value().isGreaterThanOrEqualTo(75).negate();

    final long count = getRecordStream().filter((gt100.or(gte50.and(lt75)))).count();

    Assert.assertEquals(925L, count);
    verify(data, times(926)).get(ArgumentMatchers.any());
  }

  @Test
  public void testConjunctionWithDisjunctionsAndNestedNegate() {
    final Predicate<Record<?>> gt500 = def.value().isGreaterThan(500);
    final Predicate<Record<?>> lt700 = def.value().isLessThan(700);
    final Predicate<Record<?>> lte500 = gt500.negate();
    final Predicate<Record<?>> gte700 = lt700.negate();

    final long count = getRecordStream().filter((lte500.and(gte700)).negate()).count();

    Assert.assertEquals(1001L, count);
    verify(data, times(1001)).get(ArgumentMatchers.any());
  }

  @Test
  public void testBoundPredicateWithBlind() {
    Predicate<Record<?>> lt = def.value().isLessThan(100);
    Predicate<Record<?>> gt = def.value().isGreaterThanOrEqualTo(91);
    long count = getRecordStream().filter(lt).filter(gt).filter(r -> (r.get(def).orElse(0) == 94)).count();
    Assert.assertEquals(1L, count);
    verify(data, times(10)).get(ArgumentMatchers.any());
  }

  @Test
  public void testBlindPredicate() {
    long count = getRecordStream().filter(r -> r.get(def).orElse(0) < 10).count();
    verify(data).first(data.start(true));
    Assert.assertEquals(10L, count);
    verify(data, times(1001)).get(ArgumentMatchers.any());
  }

  @Test
  public void testIndexSelection() {
    Predicate<Record<?>> lt = def.value().isLessThan(5);
    long count = getRecordStream().filter(lt).count();
    Assert.assertEquals(5L, count);
    verify(data, times(5)).get(ArgumentMatchers.any());
    Predicate<Record<?>> gt = def.value().isGreaterThan(995);
    count = getRecordStream().filter(gt).count();
    Assert.assertEquals(5L, count);
    verify(data, times(10)).get(ArgumentMatchers.any());
    Predicate<Record<?>> gte = def.value().isGreaterThanOrEqualTo(995);
    count = getRecordStream().filter(gte).count();
    Assert.assertEquals(6L, count);
    verify(data, times(16)).get(ArgumentMatchers.any());
    Predicate<Record<?>> lte = def.value().isLessThanOrEqualTo(5);
    count = getRecordStream().filter(lte).count();
    Assert.assertEquals(6L, count);
    verify(data, times(22)).get(ArgumentMatchers.any());
    Predicate<Record<?>> is = def.value().is(5);
    count = getRecordStream().filter(is).count();
    Assert.assertEquals(1L, count);
    verify(data, times(24)).get(ArgumentMatchers.any());
  }

  @Test
  public void testNotEquals() {
    BuildablePredicate<Record<?>> neq = def.value().is(2).negate();
    BuildablePredicate<Record<?>> geq = def.value().isGreaterThanOrEqualTo(0);
    BuildablePredicate<Record<?>> leq = def.value().isLessThanOrEqualTo(4);
    BuildablePredicate<Record<?>> and = geq.and(neq).and(leq);

    long count = getRecordStream().filter(and).count();
    Assert.assertEquals(4, count);
    verify(data, times(6)).get(ArgumentMatchers.any());
  }

  @Test
  public void testNotEqualsContradiction() {
    BuildablePredicate<Record<?>> eq = def1.value().is(10);
    BuildablePredicate<Record<?>> neq = def1.value().is(10).negate();
    BuildablePredicate<Record<?>> filter = eq.and(neq);

    long count = getRecordStream().filter(filter).count();
    Assert.assertEquals(0, count);
    verify(data, never()).get(ArgumentMatchers.any());
  }

  @Test
  public void testInequalityContradiction() {
    BuildableComparableOptionalFunction<Record<?>, Integer> value = def1.value();
    BuildablePredicate<Record<?>> gt = value.isGreaterThan(10);
    BuildablePredicate<Record<?>> lt = def1.value().isLessThan(10);
    BuildablePredicate<Record<?>> filter = gt.and(lt);

    long count = getRecordStream().filter(filter).count();
    Assert.assertEquals(0, count);
    verify(data, never()).get(ArgumentMatchers.any());
  }

  @Test
  public void testInfiniteUnion() {
    final Predicate<Record<?>> gt500 = def.value().isGreaterThan(500);
    final Predicate<Record<?>> lt700 = def.value().isLessThan(700);

    final long count = getRecordStream().filter(gt500.or(lt700)).count();

    Assert.assertEquals(1001L, count);
    verify(data, times(1001)).get(ArgumentMatchers.any());
  }

  @Test
  public void testFiniteUnion() {
    final Predicate<Record<?>> gt0 = def.value().isGreaterThan(0);
    final Predicate<Record<?>> lt10 = def.value().isLessThan(10);

    final Predicate<Record<?>> gt5 = def.value().isGreaterThan(5);
    final Predicate<Record<?>> lte15 = def.value().isLessThanOrEqualTo(15);

    final long count = getRecordStream().filter(gt0.and(lt10).or(gt5.and(lte15))).count();

    Assert.assertEquals(15L, count);
    verify(data, times(16)).get(ArgumentMatchers.any());
  }

  @Test
  public void testDisruptedUnion() {
    final Predicate<Record<?>> gt10 = def.value().isGreaterThan(10);
    final Predicate<Record<?>> lt10 = def.value().isLessThan(10);

    final long count = getRecordStream().filter(gt10.or(lt10)).count();

    Assert.assertEquals(1000L, count);
    verify(data, times(1000)).get(ArgumentMatchers.any());
  }

  @Test
  public void testGroupingBy() {
    Map<Integer, Long> map = getRecordStream()
            .filter(def.intValueOrFail().isLessThan(10))
            .collect(groupingBy(def.valueOrFail(), counting()));
    Assert.assertEquals(10, map.size());
  }

  @Test
  public void testGroupingByConcurrent() {
    ConcurrentMap<Integer, Long> map = getRecordStream()
            .filter(def.intValueOrFail().isLessThan(10))
            .collect(groupingByConcurrent(def.valueOrFail(), counting()));
    Assert.assertEquals(10, map.size());
  }

  @Test
  public void testPartitioningBy() {
    Map<Boolean, Long> map = getRecordStream()
            .collect(partitioningBy(def.valueOrFail().isLessThan(10), counting()));
    Assert.assertThat(map.keySet(), containsInAnyOrder(true, false));
    Assert.assertThat(map.get(true), is(10L));
    Assert.assertThat(map.get(false), is(991L));
  }

  @Test
  public void testPartitioningWithStatistics() {
    Map<Boolean, IntSummaryStatistics> map = getRecordStream()
            .collect(partitioningBy(def.valueOrFail().isLessThan(10),
                    summarizingInt(def.intValueOr(0))));
    Assert.assertThat(map.keySet(), containsInAnyOrder(true, false));
    Assert.assertThat(map.get(true).getCount(), is(10L));
    Assert.assertThat(map.get(false).getCount(), is(991L));
  }

  @Test
  public void testMapping() {
    long count = getRecordStream()
            .collect(mapping(def.intValueOrFail().increment().boxed(),
                    counting()));
    Assert.assertEquals(1001L, count);
  }

  @Test
  public void testFiltering() {
    long count = getRecordStream()
            .collect(filtering(def.intValueOrFail().isLessThan(10),
                    counting()));
    Assert.assertEquals(10L, count);
  }

  static class MockLocator extends PersistentMemoryLocator {

    private final int index;
    private final int last;
    private final TraversalDirection direction;

    MockLocator(TraversalDirection direction, int index, int end) {
      super(index,null);
      this.index = index;
      this.direction=direction;
      this.last = end;
    }

    @Override
    public PersistentMemoryLocator next() {
      if(direction.isForward()) {
        if (index > last) {
          throw new ArrayIndexOutOfBoundsException("invalid");
        }
        return new MockLocator(direction, index + 1, last);
      } else if(direction.isReverse()) {
        if (index < 0) {
          throw new ArrayIndexOutOfBoundsException("invalid");
        }
        return new MockLocator(direction, index-1, last);
      }
      return PersistentMemoryLocator.INVALID;
    }

    @Override
    public boolean isEndpoint() {
      return index == -1 || index == last+1;
    }

    @Override
    public TraversalDirection direction() {
      return direction;
    }

    public int getIndex() {
      return index;
    }

  }
}
