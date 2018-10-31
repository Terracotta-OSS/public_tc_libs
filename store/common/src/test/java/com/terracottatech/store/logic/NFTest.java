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
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.Negation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Test normal forms.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(MockitoJUnitRunner.class)
public abstract class NFTest {

  private static final IntrinsicPredicate<?> ALWAYS_TRUE = AlwaysTrue.alwaysTrue();
  private static final IntrinsicPredicate<?> ALWAYS_FALSE = ALWAYS_TRUE.negate();

  @Mock
  IntrinsicPredicate a;

  @Mock
  IntrinsicPredicate b;

  @Mock
  IntrinsicPredicate c;

  @Mock
  IntrinsicPredicate d;

  private NormalForm.Type type;

  NFTest(NormalForm.Type type) {
    this.type = type;
  }

  @Before
  public void setRandomIntrinsicTypes() {
    when(a.getIntrinsicType()).thenReturn(IntrinsicType.PREDICATE_GATED_EQUALS);
    when(b.getIntrinsicType()).thenReturn(IntrinsicType.PREDICATE_GATED_CONTRAST);
    when(c.getIntrinsicType()).thenReturn(IntrinsicType.PREDICATE_RECORD_EQUALS);
    when(d.getIntrinsicType()).thenReturn(IntrinsicType.PREDICATE_CONTRAST);

    when(a.negate()).thenReturn(new Negation<>(a));
    when(b.negate()).thenReturn(new Negation<>(b));
    when(c.negate()).thenReturn(new Negation<>(c));
  }

  void modifyAndAssertResult(IntrinsicPredicate input, NormalForm expectedForm) {
    NormalForm actualResult = new BooleanAnalyzer<>()
            .toNormalForm(input, type);
    assertEquals(expectedForm, actualResult);
  }

  NormalForm.Builder builder() {
    return NormalForm.builder(type);
  }

  NormalForm constant(boolean value) {
    return NormalForm.constant(type, value);
  }

  @Test
  public void testNode() {
    IntrinsicPredicate input = a;
    NormalForm expectedForm = builder()
            .addClause(a)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  @Test
  public void testNegatedNode() {
    IntrinsicPredicate input = a.negate();
    NormalForm expectedForm = builder()
            .addClause(a.negate())
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * a & !b -> False
   */
  @Test
  public void testLogicalContradiction() {
    IntrinsicPredicate input = new BinaryBoolean.And(a, a.negate());
    NormalForm expectedForm = constant(false);
    modifyAndAssertResult(input, expectedForm);
  }

  /**
   * a | !a -> True
   */
  @Test
  public void testLogicalTautology() {
    IntrinsicPredicate input = new BinaryBoolean.Or(a, a.negate());
    NormalForm expectedForm = constant(true);
    modifyAndAssertResult(input, expectedForm);
  }

  @Test
  public void testAndAlwaysTrue() {
    BinaryBoolean.And input = new BinaryBoolean.And<>(ALWAYS_TRUE, a);
    NormalForm expectedForm = builder()
            .addClause(a)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }

  @Test
  public void testAndAlwaysFalse() {
    BinaryBoolean.And input = new BinaryBoolean.And<>(ALWAYS_FALSE, a);
    NormalForm expectedForm = constant(false);
    modifyAndAssertResult(input, expectedForm);
  }

  @Test
  public void testOrAlwaysTrue() {
    BinaryBoolean.Or input = new BinaryBoolean.Or<>(ALWAYS_TRUE, a);
    NormalForm expectedForm = constant(true);
    modifyAndAssertResult(input, expectedForm);
  }

  @Test
  public void testOrAlwaysFalse() {
    BinaryBoolean.Or input = new BinaryBoolean.Or<>(ALWAYS_FALSE, a);
    NormalForm expectedForm = builder()
            .addClause(a)
            .build();
    modifyAndAssertResult(input, expectedForm);
  }
}