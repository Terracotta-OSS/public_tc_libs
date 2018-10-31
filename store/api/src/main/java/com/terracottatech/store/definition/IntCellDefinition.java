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
import com.terracottatech.store.function.BuildableToIntFunction;
import com.terracottatech.store.internal.function.Functions;

import java.util.NoSuchElementException;

/**
 * Definition of a {@link com.terracottatech.store.Type#INT integer} cell.
 */
public interface IntCellDefinition extends ComparableCellDefinition<Integer> {

  /**
   * Returns a function that extracts this cells value from a record.
   * <p>
   * If this cell is absent from a record then the returned value is sourced from the given supplier.
   *
   * @param otherwise supplier to use in this cells absence
   * @return function extracting this cell
   */
  default BuildableToIntFunction<Record<?>> intValueOr(int otherwise) {
    return Functions.intValueOr(this, otherwise);
  }

  /**
   * Returns a function that extracts this cells value from a record.
   * <p>
   * If this cell is absent from a record then a {@link NoSuchElementException} is thrown.
   *
   * @return function extracting this cell
   */
  default BuildableToIntFunction<Record<?>> intValueOrFail() {
    return Functions.intValueOrFail(this);
  }
}
