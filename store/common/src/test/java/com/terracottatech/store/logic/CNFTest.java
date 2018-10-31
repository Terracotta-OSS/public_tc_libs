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

import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import org.junit.Assert;
import org.junit.Test;

/**
 * CNFTest
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CNFTest extends NFTest {

  public CNFTest() {
    super(NormalForm.Type.CONJUNCTIVE);
  }

  /**
   * (a) & (b)
   */
  @Test
  public void testAnd() {
    BinaryBoolean.And input = new BinaryBoolean.And(a, b);
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * (a | b)
   */
  @Test
  public void testOr() {
    BinaryBoolean.Or input = new BinaryBoolean.Or(a, b);
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that a | (b & c) -> (a | b) & (a | c).
   */
  @Test
  public void testNodeOrAnd() {
    IntrinsicPredicate input = new BinaryBoolean.Or(a,
            new BinaryBoolean.And(b, c));
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .addClause(a, c)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that (a & b) | c -> (a | c) & (b| c)
   */
  @Test
  public void testAndOrNode() {
    IntrinsicPredicate input = new BinaryBoolean.Or(
            new BinaryBoolean.And(a, b), c);
    NormalForm expectedForm = builder()
            .addClause(a, c)
            .addClause(b, c)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that (a & b) | (c & b) -> (a | c) & (b | c) & (a | d) & (b | d).
   */
  @Test
  public void testAndOrAnd() {
    IntrinsicPredicate input = new BinaryBoolean.Or(
            new BinaryBoolean.And(a, b),
            new BinaryBoolean.And(c, d));
    NormalForm expectedForm = builder()
            .addClause(a, c)
            .addClause(b, c)
            .addClause(a, d)
            .addClause(b, d)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that a & (b | (c & d)) -> a & (b | c) & (b | d)
   */
  @Test
  public void testNodeAndSubtree() {
    BinaryBoolean.And input = new BinaryBoolean.And(a,
            new BinaryBoolean.Or(b, new BinaryBoolean.And(c, d)));
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b, c)
            .addClause(b, d)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that a | ^(b | c) -> a | (^b & ^c) -> (a | ^b) & (a | ^c).
   */
  @Test
  public void testNodeOrNegatedOr() {
    IntrinsicPredicate input = new BinaryBoolean.Or(a,
            new BinaryBoolean.Or(b, c).negate());
    NormalForm expectedForm = builder()
            .addClause(a, b.negate())
            .addClause(a, c.negate())
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that (a & b) | (a & c) ->
   * a & (b | c)
   */
  @Test
  public void testNonRedundantResult() {
    IntrinsicPredicate input = new BinaryBoolean.Or(
            new BinaryBoolean.And(a, b),
            new BinaryBoolean.And(a, c));
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b, c)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * Test that (a & b) | (a & c) | (a & c) ->
   * a & (b | c)
   */
  @Test
  public void testTripleDnfToCnf() {
    IntrinsicPredicate input = new BinaryBoolean.Or(
            new BinaryBoolean.And(a, b),
            new BinaryBoolean.Or(
                    new BinaryBoolean.And(a, c),
                    new BinaryBoolean.And(a, c))
    );
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b, c)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * b & (a | !a) -> b
   */
  @Test
  public void testLogicalTautologyInClause() {
    IntrinsicPredicate input = new BinaryBoolean.And(b,
            new BinaryBoolean.Or(a, a.negate()));
    NormalForm expectedForm = builder()
            .addClause(b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * b & (a & !a) -> false
   */
  @Test
  public void testLogicalContradictionInClause() {
    IntrinsicPredicate input = new BinaryBoolean.And(b,
            new BinaryBoolean.And(a, a.negate()));
    NormalForm expectedForm = constant(false);
    Assert.assertTrue(expectedForm.isContradiction());
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * (a | b) & ((a | b) | c) -> a | b
   */
  @Test
  public void testAbsorbAndOverOr() {
    BinaryBoolean.Or ab = new BinaryBoolean.Or(a, b);
    BinaryBoolean.Or abc = new BinaryBoolean.Or(ab, c);
    IntrinsicPredicate input = new BinaryBoolean.And(ab, abc);
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * (a & b) | ((a & b) & c) -> a & b
   */
  @Test
  public void testAbsorbOrOverAnd() {
    BinaryBoolean.And ab = new BinaryBoolean.And(a, b);
    BinaryBoolean.And abc = new BinaryBoolean.And(ab, c);
    IntrinsicPredicate input = new BinaryBoolean.Or(ab, abc);
    NormalForm expectedForm = builder()
            .addClause(a)
            .addClause(b)
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
            .addClause(a, b)
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
            .addClause(a)
            .addClause(b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * ((a | (b & c)) | (b | (a & c))) => (a | b)
   */
  @Test
  public void testDeeplyNested() {
    BinaryBoolean.Or input = new BinaryBoolean.Or(
            new BinaryBoolean.Or(a, new BinaryBoolean.And(b, c)),
            new BinaryBoolean.Or(b, new BinaryBoolean.And(a, c))
    );
    NormalForm expectedForm = builder()
            .addClause(a, b)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }
}
