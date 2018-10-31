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
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.EQ;

/**
 * Implements intrinsic comparision operations which directly compare the results of the
 * value-deriving functions provided.
 *
 * @param <T> the input type of the value deriving function
 * @param <U> the type to compare -- the output type of the value deriving function
 *
 * @see GatedComparison
 */
public abstract class NonGatedComparison<T, U> implements IntrinsicPredicate<T> {

  protected final IntrinsicFunction<T, U> left;
  protected final IntrinsicFunction<T, U> right;
  private final ComparisonType comparisonType;

  private NonGatedComparison(IntrinsicFunction<T, U> left, ComparisonType comparisonType, IntrinsicFunction<T, U> right) {
    this.left = left;
    this.right = right;
    this.comparisonType = comparisonType;
  }

  @Override
  public boolean test(T t) {
    return getComparisonType().evaluate(getLeft().apply(t), getRight().apply(t));
  }

  public IntrinsicFunction<T, U> getLeft() {
    return left;
  }

  public IntrinsicFunction<T, U> getRight() {
    return right;
  }

  @Override
  public List<Intrinsic> incoming() {
    return Arrays.asList(getRight(), getLeft());
  }

  public ComparisonType getComparisonType() {
    return comparisonType;
  }

  @Override
  public String toString(Function<Intrinsic, String> formatter) {
    return "(" + getLeft().toString(formatter) + getComparisonType().toString() + getRight().toString(formatter) + ")";
  }

  @Override
  public String toString() {
    return toString(Object::toString);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NonGatedComparison<?, ?> that = (NonGatedComparison<?, ?>) o;
    return comparisonType == that.comparisonType &&
            Objects.equals(left, that.left) &&
            Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right, comparisonType);
  }

  /**
   * Implements a non-gated equality operation.
   * @param <T> the input type of the value deriving function
   * @param <U> the type to compare -- the output type of the value deriving function
   */
  public static class Equals<T, U> extends NonGatedComparison<T, U> {

    public Equals(IntrinsicFunction<T, U> left, IntrinsicFunction<T, U> right) {
      super(left, EQ, right);
    }

    @Override
    public IntrinsicType getIntrinsicType() {
      return IntrinsicType.PREDICATE_EQUALS;
    }
  }

  /**
   * Implements non-gated, ordering relational operations.
   * @param <T> the input type of the value deriving function
   * @param <U> the type to compare -- the output type of the value deriving function
   */
  public static class Contrast<T, U extends Comparable<U>> extends NonGatedComparison<T, U> {

    public Contrast(IntrinsicFunction<T, U> left, ComparisonType comparisonType, IntrinsicFunction<T, U> right) {
      super(left, comparisonType, right);
    }

    @Override
    public IntrinsicType getIntrinsicType() {
      return IntrinsicType.PREDICATE_CONTRAST;
    }

    @Override
    public IntrinsicPredicate<T> negate() {
      return new Contrast<>(getLeft(), getComparisonType().negate(), getRight());
    }
  }

}
