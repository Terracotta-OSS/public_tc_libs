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
package com.terracottatech.store.definition;

import com.terracottatech.store.Record;
import com.terracottatech.store.function.BuildablePredicate;

/**
 * Definition of a {@link com.terracottatech.store.Type#BOOL boolean} cell.
 */
public interface BoolCellDefinition extends ComparableCellDefinition<Boolean> {

  /**
   * Returns a record predicate derived from this cell value.
   * <p>
   * If this cell is absent from a record then the predicate evaluates to false.
   *
   * @return a record predicate of this cell
   */
  default BuildablePredicate<Record<?>> isTrue() {
    return value().is(true);
  }

  /**
   * Returns a record predicate derived from the logical inverse of this cell value.
   * <p>
   * If this cell is absent from a record then the predicate evaluates to false.
   *
   * @return a record predicate of the inverse of this cell
   */
  default BuildablePredicate<Record<?>> isFalse() {
    return value().is(false);
  }
}
