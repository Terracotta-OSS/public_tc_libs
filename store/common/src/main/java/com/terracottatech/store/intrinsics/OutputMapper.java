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

import java.util.function.BiFunction;
import java.util.function.Function;

import static com.terracottatech.store.intrinsics.IntrinsicType.OUTPUT_MAPPER;
import static java.util.Objects.requireNonNull;

/**
 * A {@link BiFunction} that applies a given function to the <i>second</i> argument
 * passed to this {@code BiFunction}.
 */
public class OutputMapper<T, U, R>
    extends LeafIntrinsic
    implements IntrinsicBiFunction<T, U, R> {

  private final IntrinsicFunction<U, R> outputMappingFunction;

  public OutputMapper(IntrinsicFunction<U, R> outputMappingFunction) {
    super(OUTPUT_MAPPER);
    this.outputMappingFunction = requireNonNull(outputMappingFunction, "outputMappingFunction");
  }

  public IntrinsicFunction<U, R> getMapper() {
    return outputMappingFunction;
  }

  @Override
  public R apply(T t, U u) {
    return outputMappingFunction.apply(u);
  }

  @Override
  public <V> BiFunction<T, U, V> andThen(Function<? super R, ? extends V> after) {
    requireNonNull(after, "after");
    Function<U, V> andThen = outputMappingFunction.andThen(after);
    if (andThen instanceof Intrinsic) {
      @SuppressWarnings("unchecked")
      IntrinsicFunction<T, V> intrinsicFunction = (IntrinsicFunction<T, V>) andThen;
      return new InputMapper<>(intrinsicFunction);
    }
    return (T t, U u) -> andThen.apply(u);
  }

  @Override
  public String toString() {
    return "output()." + outputMappingFunction.toString();
  }
}
