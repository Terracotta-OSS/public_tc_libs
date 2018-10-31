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
package com.terracottatech.store.function;

import static com.terracottatech.store.internal.function.GatedPredicate.gated;

/**
 * A function of one argument that generates an {@code Optional<Comparable>} result, and
 * supports building derived functional types.
 *
 * @author cdennis
 */
public interface BuildableComparableOptionalFunction<T, R extends Comparable<R>> extends BuildableOptionalFunction<T, R> {

  /**
   * Returns a predicate that tests the value generated by {@code this} to see
   * if it is <em>greater than</em> the supplied constant. If {@code this} is
   * a function that generates an empty {@code Optional}, the returned predicate
   * would always evaluate to false.
   *
   * @param test value to test against
   * @return a greater than predicate
   */
  default BuildablePredicate<T> isGreaterThan(R test) {
    return gated(t -> apply(t).isPresent(), t -> apply(t).map(c -> c.compareTo(test) > 0).orElse(false));
  }

  /**
   * Returns a predicate that tests the value generated by {@code this} to see
   * if it is <em>less than</em> the supplied constant. If {@code this} is a
   * function that generates an empty {@code Optional}, the returned predicate
   * would always evaluate to false
   *
   * @param test value to test against
   * @return a less than predicate
   */
  default BuildablePredicate<T> isLessThan(R test) {
    return gated(t -> apply(t).isPresent(), t -> apply(t).map(c -> c.compareTo(test) < 0).orElse(false));
  }

  /**
   * Returns a predicate that tests the value generated by {@code this} to see
   * if it is <em>greater than or equal to</em> the supplied constant.
   * If {@code this} is a function that generates an empty {@code Optional},
   * the returned predicate would always evaluate to false
   *
   * @param test value to test against
   * @return a greater than or equal to predicate
   */
  default BuildablePredicate<T> isGreaterThanOrEqualTo(R test) {
    return gated(t -> apply(t).isPresent(), t -> apply(t).map(c -> c.compareTo(test) >= 0).orElse(false));
  }

  /**
   * Returns a predicate that tests the value generated by {@code this} to see
   * if it is <em>less than or equal to</em> the supplied constant.
   * If {@code this} is a function that generates an empty {@code Optional},
   * the returned predicate would always evaluate to false
   *
   * @param test value to test against
   * @return a less than or equal to predicate
   */
  default BuildablePredicate<T> isLessThanOrEqualTo(R test) {
    return gated(t -> apply(t).isPresent(), t -> apply(t).map(c -> c.compareTo(test) <= 0).orElse(false));
  }
}
