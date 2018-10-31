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
package com.terracottatech.store.intrinsics;

import com.terracottatech.store.function.BuildableComparableFunction;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.function.BuildableToDoubleFunction;
import com.terracottatech.store.intrinsics.impl.Add;
import com.terracottatech.store.intrinsics.impl.Divide;
import com.terracottatech.store.intrinsics.impl.Multiply;
import com.terracottatech.store.intrinsics.impl.ToDoubleFunctionAdapter;

public interface IntrinsicBuildableToDoubleFunction<T> extends IntrinsicToDoubleFunction<T>, BuildableToDoubleFunction<T> {

  @Override
  default BuildablePredicate<T> is(double test) {
    return boxed().is(test);
  }

  @Override
  default BuildablePredicate<T> isGreaterThan(double test) {
    return boxed().isGreaterThan(test);
  }

  @Override
  default BuildablePredicate<T> isLessThan(double test) {
    return boxed().isLessThan(test);
  }

  @Override
  default BuildablePredicate<T> isGreaterThanOrEqualTo(double test) {
    return boxed().isGreaterThanOrEqualTo(test);
  }

  @Override
  default BuildablePredicate<T> isLessThanOrEqualTo(double test) {
    return boxed().isLessThanOrEqualTo(test);
  }

  @Override
  default BuildableComparableFunction<T, Double> boxed() {
    return new ToDoubleFunctionAdapter<>(this);
  }

  @Override
  default IntrinsicBuildableToDoubleFunction<T> add(double operand) {
    return Add.Double.add(this, operand);
  }

  @Override
  default IntrinsicBuildableToDoubleFunction<T> multiply(double operand) {
    return Multiply.Double.multiply(this, operand);
  }

  @Override
  default IntrinsicBuildableToDoubleFunction<T> divide(double operand) {
    return Divide.Double.divide(this, operand);
  }
}
