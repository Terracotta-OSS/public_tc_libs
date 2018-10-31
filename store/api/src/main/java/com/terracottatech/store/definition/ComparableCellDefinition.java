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
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.function.BuildableComparableFunction;
import com.terracottatech.store.function.BuildableComparableOptionalFunction;

import java.util.Objects;

/**
 * Definition of a comparable cell.
 *
 * @param <T> the associated {@link Comparable} JDK type
 */
public interface ComparableCellDefinition<T extends Comparable<T>> extends CellDefinition<T> {

  @Override
  default BuildableComparableOptionalFunction<Record<?>, T> value() {
    return Functions.extractComparable(this);
  }

  @Override
  default BuildableComparableFunction<Record<?>, T> valueOr(T otherwise) {
    Objects.requireNonNull(otherwise, "Otherwise value must be non-null");
    return Functions.comparableValueOr(this, otherwise);
  }

  @Override
  default BuildableComparableFunction<Record<?>, T> valueOrFail() {
    return Functions.comparableValueOrFail(this);
  }
}
