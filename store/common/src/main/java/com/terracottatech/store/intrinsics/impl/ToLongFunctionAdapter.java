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
import com.terracottatech.store.intrinsics.IntrinsicBuildableComparableFunction;
import com.terracottatech.store.intrinsics.IntrinsicToLongFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_TO_LONG_ADAPTER;
import static java.util.Collections.singletonList;

public class ToLongFunctionAdapter<T> implements IntrinsicBuildableComparableFunction<T, Long> {

  private final IntrinsicToLongFunction<T> delegate;

  public ToLongFunctionAdapter(IntrinsicToLongFunction<T> delegate) {
    this.delegate = delegate;
  }

  public IntrinsicToLongFunction<T> getDelegate() {
    return delegate;
  }

  @Override
  public IntrinsicType getIntrinsicType() {
    return FUNCTION_TO_LONG_ADAPTER;
  }

  @Override
  public List<Intrinsic> incoming() {
    return singletonList(delegate);
  }

  @Override
  public Long apply(T value) {
    return delegate.applyAsLong(value);
  }

  @Override
  public String toString(Function<Intrinsic, String> formatter) {
    return delegate.toString(formatter);
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ToLongFunctionAdapter<?> that = (ToLongFunctionAdapter<?>) o;
    return Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate);
  }
}