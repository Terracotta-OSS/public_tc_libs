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

import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;

/**
 * Expression to represent expression of the form {@code !(cellValue)}
 *
 * @author RKAV
 */
public class Negation<T> implements IntrinsicPredicate<T> {

  private final Predicate<T> gate;
  private final IntrinsicPredicate<T> expression;

  public Negation(IntrinsicPredicate<T> expression) {
    this(t -> true, expression);
  }

  Negation(GatedComparison<T, ?> expression) {
    this(t -> expression.getLeft().apply(t).isPresent(), expression);
  }

  private Negation(Predicate<T> gate, IntrinsicPredicate<T> expression) {
    this.gate = gate;
    this.expression = expression;
  }

  public IntrinsicPredicate<T> getExpression() {
    return expression;
  }

  @Override
  public String toString(Function<Intrinsic, String> formatter) {
    return "(!" + expression.toString(formatter) + ")";
  }

  @Override
  public String toString() {
    return toString(Object::toString);
  }

  @Override
  public IntrinsicType getIntrinsicType() {
    return IntrinsicType.PREDICATE_NEGATE;
  }

  @Override
  public List<Intrinsic> incoming() {
    return singletonList(getExpression());
  }

  @Override
  public boolean test(T t) {
    return gate.test(t) && !expression.test(t);
  }

  @Override
  public IntrinsicPredicate<T> negate() {
    return expression;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Negation<?> negation = (Negation<?>) o;
    return Objects.equals(expression, negation.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression);
  }
}