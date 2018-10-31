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
import com.terracottatech.store.function.BuildableToDoubleFunction;
import com.terracottatech.store.internal.function.Functions;

import java.util.NoSuchElementException;

/**
 * Definition of a {@link com.terracottatech.store.Type#DOUBLE double} cell.
 */
public interface DoubleCellDefinition extends ComparableCellDefinition<Double> {

  /**
   * Returns a function that extracts this cells value from a record.
   * <p>
   * If this cell is absent from a record then the supplied value is used
   *
   * @param otherwise value to use in this cells absence
   * @return function extracting this cell
   */
  default BuildableToDoubleFunction<Record<?>> doubleValueOr(double otherwise) {
    return Functions.doubleValueOr(this, otherwise);
  }

  /**
   * Returns a function that extracts this cells value from a record.
   * <p>
   * If this cell is absent from a record then a {@link NoSuchElementException} is thrown.
   *
   * @return function extracting this cell
   */
  default BuildableToDoubleFunction<Record<?>> doubleValueOrFail() {
    return Functions.doubleValueOrFail(this);
  }
}
