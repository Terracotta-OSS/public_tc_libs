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
import com.terracottatech.store.intrinsics.IntrinsicBuildableToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Comparator;
import java.util.Objects;

public abstract class ArithmeticOperation<T, R extends Comparable<R>, F extends Intrinsic>
        extends LeafIntrinsic
        implements IntrinsicFunction<T, R> {

  private final F function;
  private final String operatorString;

  ArithmeticOperation(IntrinsicType type, F function, String operatorString) {
    super(type);
    this.function = function;
    this.operatorString = operatorString;
  }

  public F getFunction() {
    return function;
  }

  public Comparator<T> asComparator() {
    return new ComparableComparator<>(this);
  }

  @Override
  public String toString() {
    return getFunction() + " " + operatorString + " " + getOperandString();
  }

  protected abstract String getOperandString();

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArithmeticOperation<?, ?, ?> that = (ArithmeticOperation<?, ?, ?>) o;
    return Objects.equals(function, that.function);
  }

  @Override
  public int hashCode() {
    return Objects.hash(function);
  }

  public static abstract class Int<T> extends ArithmeticOperation<T, Integer, IntrinsicBuildableToIntFunction<T>> implements IntrinsicBuildableToIntFunction<T> {

    private final int operand;

    protected Int(IntrinsicType type, String operatorString, IntrinsicBuildableToIntFunction<T> function, int operand) {
      super(type, function, operatorString);
      this.operand = operand;
    }

    public int getOperand() {
      return operand;
    }

    @Override
    public Integer apply(T t) {
      return applyAsInt(t);
    }

    @Override
    public String getOperandString() {
      return Integer.toString(operand);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Int<?> anInt = (Int<?>) o;
      return operand == anInt.operand;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), operand);
    }
  }

  public static abstract class Long<T> extends ArithmeticOperation<T, java.lang.Long, IntrinsicBuildableToLongFunction<T>> implements IntrinsicBuildableToLongFunction<T> {

    private final long operand;

    public Long(IntrinsicType type, String operatorString, IntrinsicBuildableToLongFunction<T> function, long operand) {
      super(type, function, operatorString);
      this.operand = operand;
    }

    public long getOperand() {
      return operand;
    }

    @Override
    public java.lang.Long apply(T t) {
      return applyAsLong(t);
    }

    @Override
    public String getOperandString() {
      return java.lang.Long.toString(operand);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Long<?> aLong = (Long<?>) o;
      return operand == aLong.operand;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), operand);
    }
  }

  public static abstract class Double<T> extends ArithmeticOperation<T, java.lang.Double, IntrinsicBuildableToDoubleFunction<T>> implements IntrinsicBuildableToDoubleFunction<T> {

    private final double operand;

    protected Double(IntrinsicType type, String operatorString, IntrinsicBuildableToDoubleFunction<T> function, double operand) {
      super(type, function, operatorString);
      this.operand = operand;
    }

    public double getOperand() {
      return operand;
    }

    @Override
    public java.lang.Double apply(T t) {
      return applyAsDouble(t);
    }

    @Override
    public String getOperandString() {
      return java.lang.Double.toString(operand);
    }


    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Double<?> aDouble = (Double<?>) o;
      return java.lang.Double.compare(aDouble.operand, operand) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), operand);
    }
  }
}
