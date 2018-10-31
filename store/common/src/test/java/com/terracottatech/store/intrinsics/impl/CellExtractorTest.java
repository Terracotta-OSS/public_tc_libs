
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

package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.function.Predicate;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings("unchecked")
public class CellExtractorTest {

  @Test
  public void testDescribableAndConjuction() {
    CellDefinition<Integer> def = Mockito.mock(CellDefinition.class);
    Mockito.when(def.name()).thenReturn("x");
    BuildableComparableOptionalFunction<Record<?>, Integer> cell = CellExtractor.extractComparable(def);
    Predicate<Record<?>> tester = cell.isGreaterThan(10).and(cell.isLessThan(20));

    Record<?> fake = Mockito.mock(Record.class);
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(14));
    Assert.assertTrue(tester.test(fake));
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(4));
    Assert.assertFalse(tester.test(fake));
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(31));
    Assert.assertFalse(tester.test(fake));

    IntrinsicPredicate<Record<?>> comparison = (IntrinsicPredicate<Record<?>>) tester;
    Assert.assertThat(comparison.toString(), Is.is("((x>10)&&(x<20))"));
  }

  @Test
  public void testDescribableOrDisjunction() {
    CellDefinition<Integer> def = Mockito.mock(CellDefinition.class);
    Mockito.when(def.name()).thenReturn("x");
    BuildableComparableOptionalFunction<Record<?>, Integer> cell = CellExtractor.extractComparable(def);
    Predicate<Record<?>> tester = cell.is(10).or(cell.is(20));

    Record<?> fake = Mockito.mock(Record.class);
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(10));
    Assert.assertTrue(tester.test(fake));
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(20));
    Assert.assertTrue(tester.test(fake));
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(4));
    Assert.assertFalse(tester.test(fake));

    IntrinsicPredicate<Record<?>> predicate = (IntrinsicPredicate<Record<?>>) tester;
    Assert.assertThat(predicate.toString(), Is.is("((x==10)||(x==20))"));
  }

  @Test
  public void testDescribableNotNegation() {
    CellDefinition<Integer> def = Mockito.mock(CellDefinition.class);
    Mockito.when(def.name()).thenReturn("x");
    BuildableComparableOptionalFunction<Record<?>, Integer> cell = CellExtractor.extractComparable(def);
    Predicate<Record<?>> tester = cell.isGreaterThan(10).negate();

    Record<?> fake = Mockito.mock(Record.class);
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(10));
    Assert.assertTrue(tester.test(fake));
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(20));
    Assert.assertFalse(tester.test(fake));
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(4));
    Assert.assertTrue(tester.test(fake));

    IntrinsicPredicate<Record<?>> predicate = (IntrinsicPredicate<Record<?>>)tester;
    Assert.assertThat(predicate.toString(), Is.is("(x<=10)"));
  }

  @Test
  public void testDescribableNotNegationWhenNotPresent() {
    CellDefinition<Integer> def = Mockito.mock(CellDefinition.class);
    Mockito.when(def.name()).thenReturn("x");
    BuildableComparableOptionalFunction<Record<?>, Integer> cell = CellExtractor.extractComparable(def);
    Predicate<Record<?>> tester1 = cell.isLessThanOrEqualTo(5);
    Predicate<Record<?>> tester2 = cell.isGreaterThan(5).negate();

    Record<?> fake = Mockito.mock(Record.class);
    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(6));
    Assert.assertFalse(tester1.test(fake));
    Assert.assertFalse(tester2.test(fake));

    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(5));
    Assert.assertTrue(tester1.test(fake));
    Assert.assertTrue(tester2.test(fake));

    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(of(4));
    Assert.assertTrue(tester1.test(fake));
    Assert.assertTrue(tester2.test(fake));

    Mockito.when(fake.get((CellDefinition<Integer>) ArgumentMatchers.any(CellDefinition.class))).thenReturn(empty());
    Assert.assertFalse(tester1.test(fake));
    Assert.assertFalse(tester2.test(fake));

    IntrinsicPredicate<Record<?>> comparison1 = (IntrinsicPredicate<Record<?>>)tester1;
    Assert.assertThat(comparison1.toString(), Is.is("(x<=5)"));

    IntrinsicPredicate<Record<?>> comparison2 = (IntrinsicPredicate<Record<?>>)tester2;
    Assert.assertThat(comparison2.toString(), Is.is("(x<=5)"));
  }

  @Test
  public void testDescribableCompoundStatement() {
    CellDefinition<Integer> def1 = Mockito.mock(CellDefinition.class);
    Mockito.when(def1.name()).thenReturn("x");
    CellDefinition<Integer> def2 = Mockito.mock(CellDefinition.class);
    Mockito.when(def2.name()).thenReturn("y");
    BuildableComparableOptionalFunction<Record<?>, Integer> cell1 = CellExtractor.extractComparable(def1);
    BuildableComparableOptionalFunction<Record<?>, Integer> cell2 = CellExtractor.extractComparable(def2);
    Predicate<Record<?>> tester = cell1.is(10).negate().and(cell2.isGreaterThanOrEqualTo(15).or(cell2.isLessThanOrEqualTo(5)));

    Record<?> fake = Mockito.mock(Record.class);
    Mockito.when(fake.get(def1)).thenReturn(of(11));
    Mockito.when(fake.get(def2)).thenReturn(of(16));
    Assert.assertTrue(tester.test(fake));

    Mockito.when(fake.get(def1)).thenReturn(of(10));
    Mockito.when(fake.get(def2)).thenReturn(of(4));
    Assert.assertFalse(tester.test(fake));

    Mockito.when(fake.get(def1)).thenReturn(of(10));
    Mockito.when(fake.get(def2)).thenReturn(of(6));
    Assert.assertFalse(tester.test(fake));

    Mockito.when(fake.get(def1)).thenReturn(of(8));
    Mockito.when(fake.get(def2)).thenReturn(of(4));
    Assert.assertTrue(tester.test(fake));

    IntrinsicPredicate<Record<?>> comparison = (IntrinsicPredicate<Record<?>>) tester;
    Assert.assertThat(comparison.toString(), Is.is("((!(x==10))&&((y>=15)||(y<=5)))"));
  }

  @Test
  public void testObjectMethods() {
    Object extractor = CellExtractor.extractComparable(CellDefinition.defineLong("a"));
    assertEquals(extractor, extractor);
    assertEquals(extractor.hashCode(), extractor.hashCode());

    Object same = CellExtractor.extractComparable(CellDefinition.defineLong("a"));
    assertEquals(extractor, same);
    assertEquals(extractor.hashCode(), same.hashCode());

    Object other = CellExtractor.extractComparable(CellDefinition.defineLong("b"));
    assertNotEquals(extractor, other);
  }
}
