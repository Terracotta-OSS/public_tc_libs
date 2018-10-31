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

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.bpodgursky.jbool_expressions.Not;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.Variable;
import com.terracottatech.store.Record;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.GatedComparison;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.GREATER_THAN;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.LESS_THAN;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("rawtypes")
public class BooleanExpressionBuilderTest {

  @Test
  public void testVariable() {
    TestPredicate predicate = new TestPredicate(0);
    Expression<IntrinsicWrapper<Record>> actual = new BooleanExpressionBuilder<>(predicate)
            .build();
    Expression<IntrinsicWrapper<Record>> expected = Variable.of(new IntrinsicWrapper<>(predicate, 0));
    assertExpressions(expected, actual);
  }

  @Test
  public void testNot() {
    TestPredicate predicate = new TestPredicate(0);
    Expression<IntrinsicWrapper<Record>> actual = new BooleanExpressionBuilder<>(predicate.negate())
            .build();
    Expression<IntrinsicWrapper<Record>> expected = Not.of(Variable.of(new IntrinsicWrapper<>(predicate, 0)));
    assertExpressions(expected, actual);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNotEquals() {
    IntrinsicFunction<Record, Optional<Integer>> mock = Mockito.mock(IntrinsicFunction.class);
    Constant<Record, Integer> constant = new Constant<>(0);
    IntrinsicPredicate<Record> eq = new GatedComparison.Equals<>(mock, constant);
    IntrinsicPredicate<Record> gt = new GatedComparison.Contrast<>(mock, GREATER_THAN, constant);
    IntrinsicPredicate<Record> lt = new GatedComparison.Contrast<>(mock, LESS_THAN, constant);

    Expression<IntrinsicWrapper<Record>> actual = new BooleanExpressionBuilder<>(eq.negate())
            .build();
    Expression<IntrinsicWrapper<Record>> expected = Or.of(
            Variable.of(new IntrinsicWrapper<>(gt, 0)),
            Variable.of(new IntrinsicWrapper<>(lt, 1))
    );
    assertExpressions(expected, actual);
  }

  @Test
  public void testAlwaysTrue() {
    Expression<?> actual = new BooleanExpressionBuilder<>(AlwaysTrue.alwaysTrue())
            .build();
    Expression<?> expected = Literal.getTrue();
    assertExpressions(expected, actual);
  }

  @Test
  public void testAnd() {
    TestPredicate a = new TestPredicate(0);
    TestPredicate b = new TestPredicate(1);
    BinaryBoolean.And<Record> and = new BinaryBoolean.And<>(a, b);
    Expression<?> actual = new BooleanExpressionBuilder<>(and)
            .build();
    Expression<IntrinsicWrapper<Record>> expected = And.of(
            Variable.of(new IntrinsicWrapper<>(a, 0)),
            Variable.of(new IntrinsicWrapper<>(b, 1))
    );
    assertExpressions(expected, actual);
  }

  @Test
  public void testOr() {
    TestPredicate a = new TestPredicate(0);
    TestPredicate b = new TestPredicate(1);
    BinaryBoolean.Or<Record> or = new BinaryBoolean.Or<>(a, b);
    Expression<?> actual = new BooleanExpressionBuilder<>(or)
            .build();
    Expression<IntrinsicWrapper<Record>> expected = Or.of(
            Variable.of(new IntrinsicWrapper<>(a, 0)),
            Variable.of(new IntrinsicWrapper<>(b, 1))
    );
    assertExpressions(expected, actual);
  }

  @Test
  public void testNestedAnd() {
    TestPredicate a = new TestPredicate(0);
    TestPredicate b = new TestPredicate(1);
    TestPredicate c = new TestPredicate(2);
    BinaryBoolean.And<Record> and = new BinaryBoolean.And<>(a, new BinaryBoolean.And<>(b, c));
    Expression<?> actual = new BooleanExpressionBuilder<>(and)
            .build();
    Expression<IntrinsicWrapper<Record>> expected = And.of(
            Variable.of(new IntrinsicWrapper<>(a, 0)),
            Variable.of(new IntrinsicWrapper<>(b, 1)),
            Variable.of(new IntrinsicWrapper<>(c, 2))
    );
    assertExpressions(expected, actual);
  }

  @Test
  public void testNestedOr() {
    TestPredicate a = new TestPredicate(0);
    TestPredicate b = new TestPredicate(1);
    TestPredicate c = new TestPredicate(2);
    BinaryBoolean.Or<Record> or = new BinaryBoolean.Or<>(a, new BinaryBoolean.Or<>(b, c));
    Expression<?> actual = new BooleanExpressionBuilder<>(or)
            .build();
    Expression<IntrinsicWrapper<Record>> expected = Or.of(
            Variable.of(new IntrinsicWrapper<>(a, 0)),
            Variable.of(new IntrinsicWrapper<>(b, 1)),
            Variable.of(new IntrinsicWrapper<>(c, 2))
    );
    assertExpressions(expected, actual);
  }

  @Test
  public void testNestedMix() {
    TestPredicate a = new TestPredicate(0);
    TestPredicate b = new TestPredicate(1);
    TestPredicate c = new TestPredicate(2);
    TestPredicate d = new TestPredicate(3);
    TestPredicate e = new TestPredicate(4);
    TestPredicate f = new TestPredicate(5);

    BinaryBoolean.Or<Record> or1 = new BinaryBoolean.Or<>(a, new BinaryBoolean.And<>(b, c));
    BinaryBoolean.Or<Record> or2 = new BinaryBoolean.Or<>(d, new BinaryBoolean.And<>(e, f));
    BinaryBoolean.And<Record> and = new BinaryBoolean.And<>(or1, or2);

    Expression<?> actual = new BooleanExpressionBuilder<>(and)
            .build();
    Expression<IntrinsicWrapper<Record>> expected = And.of(
            Or.of(
                    Variable.of(new IntrinsicWrapper<>(a, 0)),
                    And.of(
                            Variable.of(new IntrinsicWrapper<>(b, 1)),
                            Variable.of(new IntrinsicWrapper<>(c, 2))
                    )
            ),
            Or.of(
                    Variable.of(new IntrinsicWrapper<>(d, 3)),
                    And.of(
                            Variable.of(new IntrinsicWrapper<>(e, 4)),
                            Variable.of(new IntrinsicWrapper<>(f, 5))
                    )
            )
    );
    assertExpressions(expected, actual);
  }

  @Test
  public void testRedundantTerms() {
    TestPredicate a = new TestPredicate(0);
    TestPredicate b = new TestPredicate(1);
    BinaryBoolean.And<Record> and = new BinaryBoolean.And<>(
            a,
            new BinaryBoolean.And<>(
                    b,
                    new BinaryBoolean.And<>(a, b)
            )
    );
    Expression<?> actual = new BooleanExpressionBuilder<>(and)
            .build();
    Expression<IntrinsicWrapper<Record>> expected = And.of(
            Variable.of(new IntrinsicWrapper<>(a, 0)),
            Variable.of(new IntrinsicWrapper<>(b, 1))
    );
    assertExpressions(expected, actual);
  }

  private void assertExpressions(Expression<?> expected, Expression<?> actual) {
    assertEquals(expected.toString(), actual.toString());
    assertEquals(expected.hashCode(), actual.hashCode());
  }

  private static class TestPredicate implements IntrinsicPredicate<Record> {

    private final int num;

    TestPredicate(int num) {
      this.num = num;
    }

    @Override
    public IntrinsicType getIntrinsicType() {
      return IntrinsicType.PREDICATE_GATED_EQUALS;
    }

    @Override
    public List<Intrinsic> incoming() {
      return null;
    }

    @Override
    public String toString(Function<Intrinsic, String> formatter) {
      return null;
    }

    @Override
    public boolean test(Record record) {
      return false;
    }

    @Override
    public String toString() {
      return "{" + num + "}";
    }
  }
}
