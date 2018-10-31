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

import com.terracottatech.store.intrinsics.IntrinsicBuildableToDoubleFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicBuildableToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

public class Add {

  private Add() {
  }

  public static class Int<T> extends ArithmeticOperation.Int<T> {

    public static <T> IntrinsicBuildableToIntFunction<T> add(IntrinsicBuildableToIntFunction<T> a, int b) {
      if (b == 0) {
        return a;
      } else {
        return new Add.Int<>(a, b);
      }
    }

    public Int(IntrinsicBuildableToIntFunction<T> function, int operand) {
      super(IntrinsicType.ADD_INT, "+", function, operand);
    }

    @Override
    public int applyAsInt(T t) {
      return getFunction().applyAsInt(t) + getOperand();
    }
  }

  public static class Long<T> extends ArithmeticOperation.Long<T> {

    public static <T> IntrinsicBuildableToLongFunction<T> add(IntrinsicBuildableToLongFunction<T> a, long b) {
      if (b == 0) {
        return a;
      } else {
        return new Add.Long<>(a, b);
      }
    }

    public Long(IntrinsicBuildableToLongFunction<T> function, long operand) {
      super(IntrinsicType.ADD_LONG, "+", function, operand);
    }

    @Override
    public long applyAsLong(T t) {
      return getFunction().applyAsLong(t) + getOperand();
    }
  }

  public static class Double<T> extends ArithmeticOperation.Double<T> {

    public static <T> IntrinsicBuildableToDoubleFunction<T> add(IntrinsicBuildableToDoubleFunction<T> a, double b) {
      if (b == 0) {
        return a;
      } else {
        return new Add.Double<>(a, b);
      }
    }

    public Double(IntrinsicBuildableToDoubleFunction<T> function, double operand) {
      super(IntrinsicType.ADD_DOUBLE, "+", function, operand);
    }

    @Override
    public double applyAsDouble(T t) {
      return getFunction().applyAsDouble(t) + getOperand();
    }
  }
}
