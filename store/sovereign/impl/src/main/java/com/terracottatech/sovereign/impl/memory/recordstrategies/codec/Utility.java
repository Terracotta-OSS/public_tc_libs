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
package com.terracottatech.sovereign.impl.memory.recordstrategies.codec;

import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

/**
 * Utility functions in common with all {@code Record} codec implementations.
 *
 * @author Clifford W. Johnson
 */
final class Utility {
  private Utility() {
  }

  /**
   * Gets the {@link SovereignType} for the {@link Cell} provided.
   *
   * @param cell the {@code Cell} for which the {@code SovereignType} is needed
   *
   * @return the {@code SovereignType} for {@code cell}
   *
   * @throws EncodingException if {@code cell} has no {@code SovereignType} mapping
   */
  static SovereignType getType(final Cell<?> cell) throws EncodingException {
    final Type<?> type = cell.definition().type();
    return getType(type);
  }

  /**
   * Gets the {@link SovereignType} for the {@link Type} provided.
   *
   * @param type the TC Store {@code Type}
   *
   * @return the {@code SovereignType} corresponding to {@code type}
   *
   * @throws EncodingException if {@code type} has no {@code SovereignType} mapping
   */
  static SovereignType getType(final Type<?> type) throws EncodingException {
    final SovereignType sovereignType = SovereignType.forType(type);
    if (sovereignType == null) {
      throw new EncodingException(String.format("No SovereignType for %s", type));
    }
    return sovereignType;
  }

  /**
   * Gets the {@link SovereignType} for the JDK type.
   *
   * @param type the JDK type
   *
   * @return the {@code SovereignType} corresponding to {@code type}
   *
   * @throws EncodingException if {@code type} has no {@code SovereignType} mapping
   */
  static SovereignType getType(final Class<?> type) throws EncodingException {
    final SovereignType sovereignType = SovereignType.forJDKType(type);
    if (sovereignType == null) {
      throw new EncodingException(String.format("No SovereignType for %s", type.getName()));
    }
    return sovereignType;
  }

  /**
   * Creates a new {@link Cell} instance from the cell descriptor, name, and value provided.
   *
   * @param cellDescriptor the {@code CellDescriptor} describing the cell value type
   * @param name the name for the cell
   * @param value the value for the cell
   *
   * @return a new {@code Cell}
   */
  static Cell<Object> makeCell(final CellDescriptor cellDescriptor, final String name, final Object value) {
    @SuppressWarnings("unchecked")
    final Type<Object> type = (Type<Object>)cellDescriptor.valueType.getNevadaType();
    final CellDefinition<Object> definition = CellDefinition.define(name, type);
    return definition.newCell(value);
  }
}
