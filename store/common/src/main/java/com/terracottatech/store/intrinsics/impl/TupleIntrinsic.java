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

import com.terracottatech.store.Tuple;
import com.terracottatech.store.function.BuildableFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

public abstract class TupleIntrinsic extends LeafIntrinsic {

  TupleIntrinsic(IntrinsicType type) {
    super(type);
  }

  @SuppressWarnings("unchecked")
  public static <T> TupleIntrinsic.First<T> first() {
    return First.INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public static <U> TupleIntrinsic.Second<U> second() {
    return Second.INSTANCE;
  }

  static class First<T> extends TupleIntrinsic implements BuildableFunction<Tuple<T, ?>, T> {

    @SuppressWarnings("rawtypes")
    private static final First INSTANCE = new First<>();

    private First() {
      super(IntrinsicType.TUPLE_FIRST);
    }

    @Override
    public T apply(Tuple<T, ?> tuple) {
      return tuple.getFirst();
    }

    @Override
    public String toString() {
      return "Tuple.first()";
    }
  }

  static class Second<U> extends TupleIntrinsic implements BuildableFunction<Tuple<?, U>, U> {

    @SuppressWarnings("rawtypes")
    private static final Second INSTANCE = new Second<>();

    private Second() {
      super(IntrinsicType.TUPLE_SECOND);
    }

    @Override
    public U apply(Tuple<?, U> tuple) {
      return tuple.getSecond();
    }

    @Override
    public String toString() {
      return "Tuple.second()";
    }
  }
}
