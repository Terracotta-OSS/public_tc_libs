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

import com.terracottatech.store.intrinsics.impl.LeafIntrinsic;

import java.util.function.Function;

import static com.terracottatech.store.intrinsics.IntrinsicType.IDENTITY_FUNCTION;
import static java.util.Objects.requireNonNull;

/**
 * A {@link Function} that simply returns its argument.
 */
public class IdentityFunction<T>
    extends LeafIntrinsic
    implements IntrinsicFunction<T, T> {

  public IdentityFunction() {
    super(IDENTITY_FUNCTION);
  }

  @Override
  public T apply(T t) {
    return t;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> Function<V, T> compose(Function<? super V, ? extends T> before) {
    requireNonNull(before);
    return (Function<V, T>)before;    // unchecked
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> Function<T, V> andThen(Function<? super T, ? extends V> after) {
    requireNonNull(after);
    return (Function<T, V>)after;     // unchecked
  }

  @Override
  public String toString() {
    return "identity()";
  }
}
