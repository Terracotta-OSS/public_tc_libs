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

import com.terracottatech.store.function.BuildableComparableFunction;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.function.BuildablePredicate;

/**
 * A record is a set of {@link Cell}s associated with a key.  The set of cells
 * forming a record are unique in name.  Records are immutable and are the
 * largest atomically modifiable data structure within a dataset. Naturally
 * records should therefore correspond to the natural aggregate or atomic data
 * unit within an application.
 * <p>
 * The iteration order over {@code Cell}s in a {@code Record} is undefined.
 *
 * @param <K> the key type for the record
 *
 * @author Alex Snaps
 */
public interface Record<K extends Comparable<K>> extends CellCollection {

  /**
   * Returns the key for this record.
   *
   * @return the record key
   */
  K getKey();

  /**
   * Indicates whether some object is equal to this {@code Record}.
   * <p>
   * While conforming to the general contract established for
   * {@link Object#equals(Object) Object.equals}, implementations for
   * {@code Record} subclasses will consider the value for {@link #getKey() getKey}
   *  and the cells contained in this record instance.
   *
   * @param other the reference object against which {@code this} is compared.
   * @return {@code true} if this {@code Record} is the same as {@code other}
   *      or for which the key and contained cells are equal;
   *      {@code false} otherwise.
   * @see #hashCode()
   */
  @Override
  boolean equals(Object other);

  /**
   * Returns a hash code value for this record.
   * <p>
   * While conforming to the general contract established for
   * {@link Object#hashCode() Object.hashCode}, in concert with {@code #equals},
   * implementations for {@code Record} subclasses will consider the value for
   * {@link #getKey() getKey} and the cells contained in this record instance.
   *
   * @return a hash code for this record.
   * @see #equals(Object)
   */
  @Override
  int hashCode();

  /**
   * Returns a predicate that tests if another <code>Record</code> instance is the same as this one.
   * @return a predicate that tests if another <code>Record</code> instance is the same as this one.
   */
  default BuildablePredicate<? super Record<K>> getEqualsPredicate() {
    return this::equals;
  }

  /**
   * Returns a function that, given a record, gives the key.
   * @param <K> the key type for the record
   * @return a function that, given a record, gives the key.
   */
  static <K extends Comparable<K>> BuildableComparableFunction<Record<K>, K> keyFunction() {
    return Functions.recordKeyFunction();
  }
}
