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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class BinaryBoolean<T> implements IntrinsicPredicate<T> {

  private final IntrinsicPredicate<T> left;
  private final IntrinsicPredicate<? super T> right;

  private BinaryBoolean(IntrinsicPredicate<T> left, IntrinsicPredicate<? super T> right) {
    this.left = left;
    this.right = right;
  }

  public IntrinsicPredicate<T> getLeft() {
    return left;
  }

  public IntrinsicPredicate<? super T> getRight() {
    return right;
  }

  public abstract String getOp();

  @Override
  public List<Intrinsic> incoming() {
    return Arrays.asList(getRight(), getLeft());
  }

  @Override
  public String toString(Function<Intrinsic, String> formatter) {
    return "(" + getLeft().toString(formatter) + getOp() + getRight().toString(formatter) + ")";
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
    BinaryBoolean<?> that = (BinaryBoolean<?>) o;
    return Objects.equals(left, that.left) &&
            Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right);
  }

  public static final class And<T> extends BinaryBoolean<T> {
    public And(IntrinsicPredicate<T> left, IntrinsicPredicate<? super T> right) {
      super(left, right);
    }

    @Override
    public String getOp() {
      return "&&";
    }

    @Override
    public IntrinsicType getIntrinsicType() {
      return IntrinsicType.PREDICATE_AND;
    }

    @Override
    public boolean test(T t) {
      return getLeft().test(t) && getRight().test(t);
    }
  }

  public static final class Or<T> extends BinaryBoolean<T> {
    public Or(IntrinsicPredicate<T> left, IntrinsicPredicate<? super T> right) {
      super(left, right);
    }

    @Override
    public String getOp() {
      return "||";
    }

    @Override
    public IntrinsicType getIntrinsicType() {
      return IntrinsicType.PREDICATE_OR;
    }

    @Override
    public boolean test(T t) {
      return getLeft().test(t) || getRight().test(t);
    }
  }
}
