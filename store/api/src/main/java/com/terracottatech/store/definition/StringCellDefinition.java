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
import com.terracottatech.store.function.BuildableStringFunction;
import com.terracottatech.store.function.BuildableStringOptionalFunction;

import java.util.Objects;

/**
 * Definition of a {@link com.terracottatech.store.Type#STRING string} cell.
 */
public interface StringCellDefinition extends ComparableCellDefinition<String> {

  @Override
  default BuildableStringOptionalFunction<Record<?>> value() {
    return Functions.extractString(this);
  }

  @Override
  default BuildableStringFunction<Record<?>> valueOr(String otherwise) {
    Objects.requireNonNull(otherwise, "Otherwise value must be non-null");
    return Functions.stringValueOr(this, otherwise);
  }

  @Override
  default BuildableStringFunction<Record<?>> valueOrFail() {
    return Functions.stringValueOrFail(this);
  }
}
