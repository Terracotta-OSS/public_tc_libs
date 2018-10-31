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
package com.terracottatech.store.internal.function;

import com.terracottatech.store.function.BuildablePredicate;

import java.util.function.Predicate;

/**
 *
 * @author cdennis
 */
public class GatedPredicate<T> implements BuildablePredicate<T> {

  private final Predicate<T> gate;
  private final Predicate<T> delegate;

  private GatedPredicate(Predicate<T> gate, Predicate<T> delegate) {
    this.gate = gate;
    this.delegate = delegate;
  }

  @Override
  public BuildablePredicate<T> or(Predicate<? super T> other) {
    return gated(gate, delegate.or(other));
  }

  @Override
  public BuildablePredicate<T> negate() {
    return gated(gate, delegate.negate());
  }

  @Override
  public BuildablePredicate<T> and(Predicate<? super T> other) {
    return gated(gate, delegate.and(other));
  }

  @Override
  public boolean test(T t) {
    return gate.and(delegate).test(t);
  }

  public static <T> BuildablePredicate<T> gated(Predicate<T> gate, Predicate<T> delegate) {
    return new GatedPredicate<>(gate, delegate);
  }
}
