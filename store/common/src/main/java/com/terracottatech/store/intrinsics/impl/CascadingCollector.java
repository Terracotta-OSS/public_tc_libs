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
import com.terracottatech.store.intrinsics.IntrinsicCollector;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

abstract class CascadingCollector<T, A, R, X extends Intrinsic, Y extends IntrinsicCollector<?, ?, ?>>
        extends LeafIntrinsic implements IntrinsicCollector<T, A, R> {

  private final Collector<T, A, R> delegate;
  private final X function;
  private final Y downstream;
  private final String name;

  @SuppressWarnings("unchecked")
  CascadingCollector(IntrinsicType type, X function, Y downstream,
                     BiFunction<X, Y, Collector<T, ?, R>> delegator, String name) {
    super(type);
    this.delegate = (Collector<T, A, R>) delegator.apply(function, downstream);
    this.function = function;
    this.downstream = downstream;
    this.name = name;
  }

  public final Intrinsic getFunction() {
    return function;
  }

  public final Y getDownstream() {
    return downstream;
  }

  @Override
  public final Supplier<A> supplier() {
    return delegate.supplier();
  }

  @Override
  public final BiConsumer<A, T> accumulator() {
    return delegate.accumulator();
  }

  @Override
  public final BinaryOperator<A> combiner() {
    return delegate.combiner();
  }

  @Override
  public Function<A, R> finisher() {
    return delegate.finisher();
  }

  @Override
  public final Set<Characteristics> characteristics() {
    return delegate.characteristics();
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
    CascadingCollector<?, ?, ?, ?, ?> that = (CascadingCollector<?, ?, ?, ?, ?>) o;
    return Objects.equals(function, that.function) &&
            Objects.equals(downstream, that.downstream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), function, downstream);
  }

  @Override
  public String toString() {
    return name + '(' + function + ", " + downstream + ')';
  }
}
