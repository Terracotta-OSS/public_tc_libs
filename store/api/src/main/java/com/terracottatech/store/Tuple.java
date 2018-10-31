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
package com.terracottatech.store;

import com.terracottatech.store.function.BuildableFunction;
import com.terracottatech.store.internal.function.Functions;

/**
 * Holds two values. Useful for returning results from an update operation.
 *
 * @param <T> the type of the first value
 * @param <U> the type of the second value
 */
public final class Tuple<T, U> {
  private final T t;
  private final U u;

  /**
   * Creates a Tuple with two values.
   *
   * @param t the first value
   * @param u the second value
   * @param <T> the type of the first value
   * @param <U> the type of the second value
   * @return the new Tuple
   */
  public static final <T, U> Tuple<T, U> of(T t, U u) {
    return new Tuple<>(t, u);
  }

  private Tuple(T t, U u) {
    this.t = t;
    this.u = u;
  }

  /**
   * Returns the first value held in the Tuple.
   *
   * @return the first value
   */
  public T getFirst() {
    return t;
  }

  /**
   * Returns the second value held in the Tuple.
   *
   * @return the second value
   */
  public U getSecond() {
    return u;
  }

  public static final <T> BuildableFunction<Tuple<T, ?>, T> first() {
    return Functions.tupleFirst();
  }

  public static final <U> BuildableFunction<Tuple<?, U>, U> second() {
    return Functions.tupleSecond();
  }

  @Override
  public String toString() {
    return "{" + getFirst() + ", " + getSecond() + "}";
  }
}
