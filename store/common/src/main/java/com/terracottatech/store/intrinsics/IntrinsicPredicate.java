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
import com.terracottatech.store.intrinsics.impl.BinaryBoolean;
import com.terracottatech.store.intrinsics.impl.Negation;
import com.terracottatech.store.intrinsics.impl.PredicateFunctionAdapter;

import java.util.function.Predicate;

/**
 * Root interface of all {@link Intrinsic}s that are {@link Predicate}s.
 */
public interface IntrinsicPredicate<T> extends Intrinsic, BuildablePredicate<T> {

  @Override
  default BuildablePredicate<T> and(Predicate<? super T> other) {
    if (other instanceof IntrinsicPredicate<?>) {
      return new BinaryBoolean.And<>(this, (IntrinsicPredicate<? super T>) other);
    } else {
      return BuildablePredicate.super.and(other);
    }
  }

  @Override
  default IntrinsicPredicate<T> negate() {
    return new Negation<>(this);
  }

  @Override
  default BuildablePredicate<T> or(Predicate<? super T> other) {
    if (other instanceof IntrinsicPredicate<?>) {
      return new BinaryBoolean.Or<>(this, (IntrinsicPredicate<? super T>) other);
    } else {
      return BuildablePredicate.super.or(other);
    }
  }

  @Override
  default BuildableComparableFunction<T, Boolean> boxed() {
    return new PredicateFunctionAdapter<>(this);
  }
}