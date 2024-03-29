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
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.terracottatech.store.intrinsics.IntrinsicType.FUNCTION_PREDICATE_ADAPTER;

public class PredicateFunctionAdapter<T> implements IntrinsicBuildableComparableFunction<T, Boolean> {

  private final IntrinsicPredicate<T> delegate;

  public PredicateFunctionAdapter(IntrinsicPredicate<T> delegate) {
    this.delegate = delegate;
  }

  public IntrinsicPredicate<T> getDelegate() {
    return delegate;
  }

  @Override
  public IntrinsicType getIntrinsicType() {
    return FUNCTION_PREDICATE_ADAPTER;
  }

  @Override
  public List<Intrinsic> incoming() {
    return Collections.emptyList();
  }

  @Override
  public Boolean apply(T value) {
    return delegate.test(value);
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
    PredicateFunctionAdapter<?> that = (PredicateFunctionAdapter<?>) o;
    return Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate);
  }
}
