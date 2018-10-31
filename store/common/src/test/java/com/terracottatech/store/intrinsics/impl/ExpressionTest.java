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
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;

import org.junit.Test;

import java.util.Optional;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN_OR_EQUAL;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.EQ;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN_OR_EQUAL;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AST. These unit tests verify whether the AST tree is built correctly for
 * different kinds of predicates.
 *
 * @author RKAV
 */
public class ExpressionTest {

  @Test
  public void testSimpleConstantCompareExpressionWithCellVariable() {
    IntrinsicPredicate<Record<?>> booleanExpression = getCellBasedPredicate("c1", GREATER_THAN, 20);

    assertThat(booleanExpression, is(instanceOf(GatedComparison.Contrast.class)));
    GatedComparison.Contrast<?, ?> c = (GatedComparison.Contrast) booleanExpression;
    assertEquals(c.getComparisonType(), GREATER_THAN);
    assertEquals(((CellExtractor<?>) c.getLeft()).extracts().name(), "c1");
    assertEquals(booleanExpression.toString(), "(c1>20)");
  }

  @Test
  public void testLogicalConjunctionWithCellVariable() {
    IntrinsicPredicate<Record<?>> booleanExpression1 = getCellBasedPredicate("c1", GREATER_THAN, 20);
    IntrinsicPredicate<Record<?>> booleanExpression2 = getCellBasedPredicate("c2", LESS_THAN, 25);
    IntrinsicPredicate<Record<?>> booleanExpression = new BinaryBoolean.And<>(booleanExpression1, booleanExpression2);

    assertThat(booleanExpression, is(instanceOf(BinaryBoolean.And.class)));
    BinaryBoolean<?> b = (BinaryBoolean) booleanExpression;

    GatedComparison.Contrast<?, ?> c1 = (GatedComparison.Contrast) b.getLeft();
    GatedComparison.Contrast<?, ?> c2 = (GatedComparison.Contrast) b.getRight();

    assertEquals(((CellExtractor<?>) c1.getLeft()).extracts().name(), "c1");
    assertEquals(((CellExtractor<?>) c2.getLeft()).extracts().name(), "c2");
    assertEquals(booleanExpression.toString(), "((c1>20)&&(c2<25))");
  }

  @Test
  public void testLogicalDisjunctionWithCellVariable() {
    IntrinsicPredicate<Record<?>> booleanExpression1 = getCellBasedPredicate("c1", GREATER_THAN_OR_EQUAL, 10);
    IntrinsicPredicate<Record<?>> booleanExpression2 = getCellBasedPredicate("c2", LESS_THAN_OR_EQUAL, 15);
    IntrinsicPredicate<Record<?>> booleanExpression = new BinaryBoolean.Or<>(booleanExpression1, booleanExpression2);
    assertEquals(booleanExpression.toString(), "((c1>=10)||(c2<=15))");
  }

  @Test
  public void testNegateWithCellVariable() {
    IntrinsicPredicate<Record<?>> booleanExpression1 = getCellBasedPredicate("c1", LESS_THAN, 10);
    IntrinsicPredicate<Record<?>> booleanExpression = new Negation<>(booleanExpression1);

    Negation<?> n = (Negation) booleanExpression;
    GatedComparison.Contrast<?, ?> c = (GatedComparison.Contrast) n.getExpression();
    assertEquals(((CellExtractor<?>) c.getLeft()).extracts().name(), "c1");

    assertEquals(booleanExpression.toString(), "(!(c1<10))");
  }

  @Test
  public void testNegateWithDisjunction() {
    IntrinsicPredicate<Record<?>> booleanExpression1 = getCellBasedPredicate("c1", GREATER_THAN, 1);
    IntrinsicPredicate<Record<?>> booleanExpression2 = getCellBasedPredicate("c2", EQ, 5);
    IntrinsicPredicate<Record<?>> booleanExpression3 = new BinaryBoolean.Or<>(booleanExpression1, booleanExpression2);
    IntrinsicPredicate<Record<?>> booleanExpression = new Negation<>(booleanExpression3);

    Negation<?> n = (Negation) booleanExpression;
    assertThat(n.getExpression(), is(instanceOf(BinaryBoolean.class)));
    assertEquals(booleanExpression.toString(), "(!((c1>1)||(c2==5)))");
  }

  @Test
  public void testNegatedCopyOfGreaterThan() {
    IntrinsicPredicate<Record<?>> booleanExpression = getCellBasedPredicate("c1", GREATER_THAN, 1);
    assertThat(((GatedComparison.Contrast) booleanExpression.negate()).getComparisonType(), is(LESS_THAN_OR_EQUAL));
    assertEquals(booleanExpression.toString(), "(c1>1)");
  }

  @Test
  public void testNegatedCopyOfLessThan() {
    IntrinsicPredicate<Record<?>> booleanExpression = getCellBasedPredicate("c1", LESS_THAN, 5);
    assertThat(((GatedComparison.Contrast) booleanExpression.negate()).getComparisonType(), is(GREATER_THAN_OR_EQUAL));
    assertEquals(booleanExpression.toString(), "(c1<5)");
  }

  @Test
  public void testNegatedCopyOfGreaterThanOrEqual() {
    IntrinsicPredicate<Record<?>> booleanExpression = getCellBasedPredicate("c1", GREATER_THAN_OR_EQUAL, 2);
    assertThat(((GatedComparison.Contrast) booleanExpression.negate()).getComparisonType(), is(LESS_THAN));
    assertEquals(booleanExpression.toString(), "(c1>=2)");
  }

  @Test
  public void testNegatedCopyOfLessThanOrEqual() {
    IntrinsicPredicate<Record<?>> booleanExpression = getCellBasedPredicate("c1", LESS_THAN_OR_EQUAL, 3);
    assertThat(((GatedComparison.Contrast) booleanExpression.negate()).getComparisonType(), is(GREATER_THAN));
    assertEquals(booleanExpression.toString(), "(c1<=3)");
  }

  private IntrinsicPredicate<Record<?>> getCellBasedPredicate(String cellName, ComparisonType op, int val) {
    @SuppressWarnings("unchecked") CellDefinition<Integer> def = mock(CellDefinition.class);
    when(def.name()).thenReturn(cellName);
    BuildableComparableOptionalFunction<Record<?>, Integer> cell = CellExtractor.extractComparable(def);
    return (op == EQ) ?
            new GatedComparison.Equals<>((IntrinsicFunction<Record<?>, Optional<Integer>>) cell, new Constant<>(val)) :
            new GatedComparison.Contrast<>((IntrinsicFunction<Record<?>, Optional<Integer>>) cell, op, new Constant<>(val));
  }
}