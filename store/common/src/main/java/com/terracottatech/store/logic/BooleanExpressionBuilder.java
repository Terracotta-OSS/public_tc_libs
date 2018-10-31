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
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.ComparisonType;
import com.terracottatech.store.intrinsics.impl.Constant;
import com.terracottatech.store.intrinsics.impl.GatedComparison;
import com.terracottatech.store.intrinsics.impl.Negation;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts an IntrinsicPredicate to an n-ary boolean expression.
 * <p>
 * Implementation note: the time spent by jbool_expressions to
 * analyse an expression depends on the order of clauses which
 * is internally determined by hash codes.
 * </p>
 * <p>
 * To prevent non-determinism arising from JVM-generated hash codes,
 * the Intrinsic objects from the input expression are wrapped into
 * IntrinsicWrapper objects, and the order of occurrence in the
 * expression is used as the hash code for each of these wrapper objects.
 * This hash code is used internally in jbool_expressions library as
 * a comparator function for sorting. It is not used as a hash function.
 * </p>
 * <p>
 * A lookup map is used to ensure that a wrapper is created only once for
 * a given Intrinsic object or any object with an equal value.
 * </p>
 */
class BooleanExpressionBuilder<R> {

  private final IntrinsicPredicate<R> predicate;
  private final Map<IntrinsicPredicate<R>, Variable<IntrinsicWrapper<R>>> map = new HashMap<>();

  BooleanExpressionBuilder(IntrinsicPredicate<R> predicate) {
    this.predicate = predicate;
  }

  Expression<IntrinsicWrapper<R>> build() {
    return toExpression(predicate);
  }

  private Expression<IntrinsicWrapper<R>> toExpression(IntrinsicPredicate<R> predicate) {
    IntrinsicType intrinsicType = predicate.getIntrinsicType();
    switch (intrinsicType) {
      case PREDICATE_AND:
        return toAnd(predicate);
      case PREDICATE_OR:
        return toOr(predicate);
      case PREDICATE_NEGATE:
        return toNot(predicate);
      case PREDICATE_ALWAYS_TRUE:
        return Literal.getTrue();
      default:
        return toVariable(predicate);
    }
  }

  private Expression<IntrinsicWrapper<R>> toVariable(IntrinsicPredicate<R> predicate) {
    return map.computeIfAbsent(predicate, p -> {
      IntrinsicWrapper<R> w = new IntrinsicWrapper<>(p, map.size());
      return Variable.of(w);
    });
  }

  private <V extends Comparable<V>> Expression<IntrinsicWrapper<R>> toNot(IntrinsicPredicate<R> predicate) {
    Negation<R> negation = (Negation<R>) predicate;
    IntrinsicPredicate<R> negated = negation.getExpression();
    if (testComparableEquality(negated)) {
      @SuppressWarnings("unchecked")
      GatedComparison.Equals<R, V> gatedComparison = (GatedComparison.Equals<R, V>) negated;
      return toDisjunctionOfInequalities(gatedComparison);
    } else {
      Expression<IntrinsicWrapper<R>> expression = toExpression(negated);
      return Not.of(expression);
    }
  }

  private boolean testComparableEquality(IntrinsicPredicate<R> negated) {
    if(negated instanceof GatedComparison.Equals) {
      IntrinsicFunction<?, ?> right = ((GatedComparison.Equals) negated).getRight();
      return right instanceof Constant && ((Constant) right).getValue() instanceof Comparable;
    }
    return false;
  }

  private <V extends Comparable<V>> Expression<IntrinsicWrapper<R>> toDisjunctionOfInequalities(GatedComparison.Equals<R, V> equals) {
    IntrinsicPredicate<R> or = toDisjunctionOfInequalities(equals.getLeft(), equals.getRight());
    return toOr(or);
  }

  private <V extends Comparable<V>> IntrinsicPredicate<R> toDisjunctionOfInequalities(IntrinsicFunction<R, Optional<V>> left, IntrinsicFunction<R, V> right) {
    IntrinsicPredicate<R> lt = new GatedComparison.Contrast<>(left, ComparisonType.GREATER_THAN, right);
    IntrinsicPredicate<R> gt = new GatedComparison.Contrast<>(left, ComparisonType.LESS_THAN, right);
    return new BinaryBoolean.Or<>(lt, gt);
  }

  private Expression<IntrinsicWrapper<R>> toAnd(IntrinsicPredicate<R> predicate) {
    List<Expression<IntrinsicWrapper<R>>> expanded = expandOperator(predicate, IntrinsicType.PREDICATE_AND);
    return And.of(expanded);
  }

  private Expression<IntrinsicWrapper<R>> toOr(IntrinsicPredicate<R> predicate) {
    List<Expression<IntrinsicWrapper<R>>> expanded = expandOperator(predicate, IntrinsicType.PREDICATE_OR);
    return Or.of(expanded);
  }

  private List<Expression<IntrinsicWrapper<R>>> expandOperator(IntrinsicPredicate<R> predicate, IntrinsicType type) {
    BinaryBoolean<R> operator = (BinaryBoolean<R>) predicate;
    Set<IntrinsicPredicate<R>> expanding = new LinkedHashSet<>();
    expandOperator(operator, expanding, type);
    return expanding.stream()
            .map(this::toExpression)
            .collect(Collectors.toList());
  }

  /**
   * Note that conversion from {@code IntrinsicPredicate<? super R>} to {@code IntrinsicPredicate<R>}
   * does not result in ClassCastException even if the actual types are different.
   * The actual type should be always Record.
   */
  @SuppressWarnings("unchecked")
  private void expandOperator(BinaryBoolean<R> op, Collection<IntrinsicPredicate<R>> expanding, IntrinsicType type) {
    expandSide(op.getLeft(), expanding, type);
    expandSide((IntrinsicPredicate<R>) op.getRight(), expanding, type);
  }

  private void expandSide(IntrinsicPredicate<R> side, Collection<IntrinsicPredicate<R>> expanding, IntrinsicType type) {
    if (side.getIntrinsicType() == type) {
      BinaryBoolean<R> operator = (BinaryBoolean<R>) side;
      expandOperator(operator, expanding, type);
    } else {
      expanding.add(side);
    }
  }
}
