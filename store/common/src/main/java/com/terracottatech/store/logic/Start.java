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

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Optional;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;


/**
 * Starting endpoint of an Interval.
 * @param <V>
 */
class Start<V extends Comparable<V>> extends Bound<V> implements Comparable<Start<V>> {

  Start(V value, Inclusion inclusion) {
    super(value, inclusion);
  }

  private final Comparator<Start<V>> comparator = Comparator
          .comparing(Start<V>::getValue, nullsFirst(naturalOrder()))
          .thenComparing(Start<V>::getInclusion);

  /**
   * Compare values, so that null < any other value.
   * If values are equal, then compare inclusions: OPEN < CLOSED
   */
  @Override
  public int compareTo(@Nonnull Start<V> o) {
    return comparator.compare(this, o);
  }

  @Override
  boolean isOpen() {
    return getInclusion() == Start.Inclusion.OPEN;
  }

  @Override
  public String toString() {
    return (isOpen() ? '(' : '[') +
            Optional.ofNullable(getValue())
                    .map(String::valueOf)
                    .orElse("-inf");
  }
}
