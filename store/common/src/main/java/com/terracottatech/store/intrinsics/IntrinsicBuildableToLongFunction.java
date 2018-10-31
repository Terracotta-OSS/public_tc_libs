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
import com.terracottatech.store.function.BuildableToLongFunction;
import com.terracottatech.store.intrinsics.impl.Add;
import com.terracottatech.store.intrinsics.impl.Divide;
import com.terracottatech.store.intrinsics.impl.Multiply;
import com.terracottatech.store.intrinsics.impl.ToLongFunctionAdapter;

public interface IntrinsicBuildableToLongFunction<T> extends IntrinsicToLongFunction<T>, BuildableToLongFunction<T> {

  @Override
  default BuildablePredicate<T> is(long test) {
    return boxed().is(test);
  }

  @Override
  default BuildablePredicate<T> isGreaterThan(long test) {
    return boxed().isGreaterThan(test);
  }

  @Override
  default BuildablePredicate<T> isLessThan(long test) {
    return boxed().isLessThan(test);
  }

  @Override
  default BuildablePredicate<T> isGreaterThanOrEqualTo(long test) {
    return boxed().isGreaterThanOrEqualTo(test);
  }

  @Override
  default BuildablePredicate<T> isLessThanOrEqualTo(long test) {
    return boxed().isLessThanOrEqualTo(test);
  }

  @Override
  default BuildableComparableFunction<T, Long> boxed() {
    return new ToLongFunctionAdapter<>(this);
  }

  @Override
  default IntrinsicBuildableToLongFunction<T> add(long operand) {
    return Add.Long.add(this, operand);
  }

  @Override
  default IntrinsicBuildableToLongFunction<T> multiply(long operand) {
    return Multiply.Long.multiply(this, operand);
  }

  @Override
  default IntrinsicBuildableToLongFunction<T> divide(long operand) {
    return Divide.Long.divide(this, operand);
  }
}
