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

/**
 * Used by {@link BooleanExpressionBuilder} to wrap an IntrinsicPredicate from an
 * incoming expression, using its order of occurrence as the hash code.
 * This hash code is used internally in jbool_expressions library as
 * a comparator function for sorting. It is not used as a hash function.
 * <p>
 * Implementation note: Object#equals is not overridden for performance reasons.
 * </p>
 *
 */
class IntrinsicWrapper<R> {

  private final IntrinsicPredicate<R> intrinsic;
  private final int number;

  IntrinsicWrapper(IntrinsicPredicate<R> intrinsic, int number) {
    this.number = number;
    this.intrinsic = intrinsic;
  }

  IntrinsicPredicate<R> getIntrinsic() {
    return intrinsic;
  }

  @Override
  public int hashCode() {
    return number;
  }

  @Override
  public String toString() {
    return "{" + intrinsic + '}';
  }
}
