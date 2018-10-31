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

import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.GatedComparison;
import com.terracottatech.store.intrinsics.impl.Negation;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * DNFTest
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class DNFTest extends NFTest {

  public DNFTest() {
    super(NormalForm.Type.DISJUNCTIVE);
  }

  /**
   * (a & b)
   */
  @Test
  public void testAnd() {
    BinaryBoolean.And input = new BinaryBoolean.And(a, b);
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * (a) | (b)
   */
  @Test
  public void testOr() {
    BinaryBoolean.Or input = new BinaryBoolean.Or(a, b);
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that a & (b | c) -> (a & b) | (a & c).
   */
  @Test
  public void testNodeAndOr() {
    BinaryBoolean.And input = new BinaryBoolean.And(a,
            new BinaryBoolean.Or(b, c));
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .addClause(a, c)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that (a | b) & c -> (a & c) | (b & c)
   */
  @Test
  public void testOrAndNode() {
    BinaryBoolean.And input = new BinaryBoolean.And(
            new BinaryBoolean.Or(a, b), c);
    NormalForm expectedForm = builder()
            .addClause(a, c)
            .addClause(b, c)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that (a | b) & (c | b) ->  ((a | b) & c) | (a | b) & d) ->  (a & c) | (b & c) | (a & d) | (b & d).
   */
  @Test
  public void testOrAndOr() {
    IntrinsicPredicate input = new BinaryBoolean.And(
            new BinaryBoolean.Or(a, b),
            new BinaryBoolean.Or(c, d));
    NormalForm expectedForm = builder()
            .addClause(a, c)
            .addClause(b, c)
            .addClause(a, d)
            .addClause(b, d)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that a | (b & (c | d)) -> a | (b & c) | (b & d)
   */
  @Test
  public void testNodeAndSubtree() {
    IntrinsicPredicate input = new BinaryBoolean.Or(a,
            new BinaryBoolean.And(b,
                    new BinaryBoolean.Or(c, d)));
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b, c)
            .addClause(b, d)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that a & ^(b & c) -> a & (^b | ^c) -> (a & ^b) | (a & ^c).
   */
  @Test
  public void testNodeAndNegatedAnd() {
    IntrinsicPredicate input = new BinaryBoolean.And(a,
            new BinaryBoolean.And(b, c).negate());
    NormalForm expectedForm = builder()
            .addClause(a, b.negate())
            .addClause(a, c.negate())
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that !(a & b) -> (!a) | (!b)
   */
  @Test
  public void testNotAnd() {
    IntrinsicPredicate input = new BinaryBoolean.And(a, b)
            .negate();
    NormalForm expectedForm = builder()
            .addClause(a.negate())
            .addClause(b.negate())
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * b | (a | !a) -> True
   */
  @Test
  public void testLogicalTautologyInClause() {
    IntrinsicPredicate input = new BinaryBoolean.Or(b,
            new BinaryBoolean.Or(a, a.negate()));
    NormalForm expectedForm = constant( true);
    modifyAndAssertResult(input, expectedForm);
  }


  /**
   * b | (a & !a) -> b
   */
  @Test
  public void testLogicalContradictionInClause() {
    IntrinsicPredicate input = new BinaryBoolean.Or(b,
            new BinaryBoolean.And(a, a.negate()));
    NormalForm expectedForm = builder()
            .addClause(b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * x != 1 -> (x < 1) | (x > 1)
   */
  @Test
  public void testNotEquals() {
    GatedComparison.Equals e = Mockito.mock(GatedComparison.Equals.class);
    IntrinsicFunction left = Mockito.mock(IntrinsicFunction.class);
    when(e.getLeft()).thenReturn(left);
    Constant right = Mockito.mock(Constant.class);
    when(e.getRight()).thenReturn(right);
    when(right.getValue()).thenReturn(1);

    IntrinsicPredicate input = new Negation<>(e);
    NormalForm actualResult = new BooleanAnalyzer().toNormalForm(input, NormalForm.Type.DISJUNCTIVE);
    Stream<Clause> clauses = actualResult.clauses();
    Set<ComparisonType> operators = clauses
            .peek(clause -> assertEquals(1, clause.literals().count()))
            .map(Clause::literals)
            .map(Stream::findFirst)
            .map(optional -> optional.orElse(null))
            .peek(literal -> assertTrue(literal instanceof GatedComparison.Contrast))
            .map(literal -> (GatedComparison.Contrast) literal)
            .peek(contrast -> assertEquals(left, contrast.getLeft()))
            .peek(contrast -> assertEquals(right, contrast.getRight()))
            .map(GatedComparison::getComparisonType)
            .collect(Collectors.toSet());
    assertThat(operators, Matchers.containsInAnyOrder(ComparisonType.GREATER_THAN, ComparisonType.LESS_THAN));
  }

  /**
   * (a | b) & ((a | b) | c) -> a | b
   */
  @Test
  public void testAbsorbAndOverOr() {
    IntrinsicPredicate input = new BinaryBoolean.And(
            new BinaryBoolean.Or(a, b),
            new BinaryBoolean.Or(new BinaryBoolean.Or(a, b), c)
    );
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * (a & b) | ((a & b) & c) -> a & b
   */
  @Test
  public void testAbsorbOrOverAnd() {
    IntrinsicPredicate input = new BinaryBoolean.Or(
            new BinaryBoolean.And(a, b),
            new BinaryBoolean.And(new BinaryBoolean.And(a, b), c)
    );
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * a | (!a & b) => a | b
   */
  @Test
  public void testCollapseNegationOrOverAnd() {
    BinaryBoolean.Or input = new BinaryBoolean.Or(a,
            new BinaryBoolean.And(a.negate(), b));
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * With CollapseNegation:
   * (a | (!a & b)) & c => (a | b) & c => (a & c) | (b & c)
   *
   * without:
   * (a | (!a & b)) & c => (a & c) | (!a & b & c)
   */
  @Test
  public void testNestedCollapsableNegation() {
    BinaryBoolean.Or or = new BinaryBoolean.Or(a,
            new BinaryBoolean.And(a.negate(), b));
    BinaryBoolean.And input = new BinaryBoolean.And<>(or, c);
    NormalForm expectedForm = builder()
            .addClause(a, c)
            .addClause(b, c)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * a & (!a | b) => a & b
   */
  @Test
  public void testCollapseNegationAndOverOr() {
    BinaryBoolean.And input = new BinaryBoolean.And(a,
            new BinaryBoolean.Or(a.negate(), b));
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * ((a & (b | c)) & (b & (a | c))) => (a & b)
   */
  @Test
  public void testDeeplyNested() {
    BinaryBoolean.And input = new BinaryBoolean.And(
            new BinaryBoolean.And(a, new BinaryBoolean.Or(b, c)),
            new BinaryBoolean.And(b, new BinaryBoolean.Or(a, c))
    );
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }
}