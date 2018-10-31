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

package com.terracottatech.store.logic;

import java.util.Objects;

/**
 * Endpoint of an Interval on a totally ordered set.
 * Has a value which is null for infinite endpoints, and
 * an inclusion property.
 * @param <V> type of the set elements.
 */
abstract class Bound<V extends Comparable<V>> {

  /**
   * Inclusion property which defines whether
   * the bound is open or closed.
   *
   * Note: The order of constants in this enum is used to
   * compare intervals with equal finite values.
   */
  enum Inclusion {
    OPEN, CLOSED
  }

  private final V value;
  private final Inclusion inclusion;

  Bound(V value, Inclusion inclusion) {
    this.value = value;
    this.inclusion = inclusion;
  }

  /**
   * The value of the endpoint.
   * @return Non-null value if the endpoint is finite, else null.
   */
  V getValue() {
    return value;
  }

  /**
   * Inclusion property which defines whether
   * the interval is open or closed on the given end.
   *
   * @return Inclusion of this endpoint.
   */
  Inclusion getInclusion() {
    return inclusion;
  }

  /**
   * Whether the endpoint is finite or infinite.
   *
   * @return true if the endpoint has a non-null value, else false.
   */
  boolean isFinite() {
    return value != null;
  }

  /**
   * Interval is open at this end.
   * @return true if inclusion is set to "open" value, else false.
   */
  abstract boolean isOpen();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Bound<?> bound = (Bound<?>) o;
    return Objects.equals(value, bound.value) &&
            Objects.equals(inclusion, bound.inclusion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, inclusion);
  }
}
