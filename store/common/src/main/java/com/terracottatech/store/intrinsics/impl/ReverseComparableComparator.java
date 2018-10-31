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

import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Comparator;
import java.util.Objects;

public class ReverseComparableComparator<T, C extends Comparable<C>> extends LeafIntrinsic implements Comparator<T> {

  private final ComparableComparator<T, C> comparator;

  public ReverseComparableComparator(ComparableComparator<T, C> comparator) {
    super(IntrinsicType.REVERSE_COMPARATOR);
    this.comparator = comparator;
  }

  public ComparableComparator<T, C> getOriginalComparator() {
    return comparator;
  }

  @Override
  public int compare(T o1, T o2) {
    return comparator.compare(o2, o1);
  }

  @Override
  public Comparator<T> reversed() {
    return comparator;
  }

  @Override
  public String toString() {
    return comparator + ".reversed()";
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
    ReverseComparableComparator<?, ?> that = (ReverseComparableComparator<?, ?>) o;
    return Objects.equals(comparator, that.comparator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), comparator);
  }
}
