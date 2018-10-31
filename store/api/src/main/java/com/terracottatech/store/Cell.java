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

import com.terracottatech.store.definition.CellDefinition;

import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.Type.forJdkType;
import static java.util.Objects.requireNonNull;

/**
 * A representation of a cell.  A cell is the smallest data storage unit within
 * a dataset.  A cell couples a {@link CellDefinition} with a non-null value
 * corresponding to the definitions type.
 *
 * @param <T> the cell value type
 *
 * @author Alex Snaps
 */
public interface Cell<T> {

  /**
   * Returns the definition for this cell.
   *
   * @return the cell definition
   */
  CellDefinition<T> definition();

  /**
   * Returns the value held within this cell.
   *
   * @return the cell value
   */
  T value();

  /**
   * Compares the supplied object with this cell for equality.
   *
   * Returns {@code true} if the supplied object is a cell with same definition and value.
   *
   * @param o object to be compared for equality with this cell
   * @return {@code true} if the given object is equal to this cell
   */
  boolean equals(Object o);

  /**
   * Creates a new cell.
   * <p>
   * The returned cell has definition with the supplied name, and a type derived
   * from the type of the supplied value.
   *
   * @param <T> JDK type of the returned cell
   * @param name cell definition name
   * @param value cell value
   * @return a new cell
   */
  public static <T> Cell<T> cell(final String name, final T value) {
    requireNonNull(value, "Cell value must be non-null");
    @SuppressWarnings("unchecked")
    Type<T> type = forJdkType((Class<? extends T>) value.getClass());
    if (type == null) {
      throw new IllegalArgumentException("No support for type: " + value.getClass());
    } else {
      return define(name, type).newCell(value);
    }
  }
}
