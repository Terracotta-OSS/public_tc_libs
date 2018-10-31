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

import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Objects;

public class Constant<T, R> extends LeafIntrinsic implements IntrinsicFunction<T, R> {

  private final R constant;

  public Constant(R constant) {
    super(IntrinsicType.FUNCTION_CONSTANT);
    this.constant = constant;
  }

  @Override
  public R apply(T o) {
    return constant;
  }

  public R getValue() {
    return constant;
  }

  @Override
  public String toString() {
    return constant.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Constant<?, ?> constant1 = (Constant<?, ?>) o;
    return Objects.equals(constant, constant1.constant);
  }

  @Override
  public int hashCode() {
    return Objects.hash(constant);
  }
}
