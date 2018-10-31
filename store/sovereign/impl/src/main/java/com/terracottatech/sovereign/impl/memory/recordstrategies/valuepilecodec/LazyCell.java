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
package com.terracottatech.sovereign.impl.memory.recordstrategies.valuepilecodec;

import com.terracottatech.sovereign.common.valuepile.RandomValuePileReader;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;

/**
 * @author cschanck
 **/
class LazyCell<T> implements Cell<T> {
  private final LazyValuePileSingleRecord<?> parent;
  private volatile CellDefinition<T> def;
  private final int valueIndex;
  private final int defIndex;
  private volatile T value;

  public LazyCell(CellDefinition<T> def, T value) {
    this.def = def;
    this.value = value;
    valueIndex = -1;
    defIndex = -1;
    parent = null;
  }

  public LazyCell(LazyValuePileSingleRecord<?> rec, int defIndex, int valueIndex) {
    this.defIndex = defIndex;
    this.valueIndex = valueIndex;
    this.parent = rec;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CellDefinition<T> definition() {
    if (def == null) {
      RandomValuePileReader r = parent.getReader();
      def = (CellDefinition<T>) ValuePileBufferReader.readCellDefinition(r, parent.getSchema(), defIndex);
    }
    return def;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T value() {
    if (value == null) {
      RandomValuePileReader r = parent.getReader();
      value = (T) ValuePileBufferReader.readCellValueOnly(r, definition().type(), valueIndex);
    }
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Cell<?>) {
      Cell<?> other = (Cell<?>) obj;
      return definition().equals(other.definition()) && Type.equals(value(), other.value());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return definition().hashCode() + 31 * value().hashCode();
  }

  @Override
  public String toString() {
    return toString(true);
  }

  /**
   * A {@code toString} variant permitting a choice of materializing lazy cell content.
   * {@code toString(false)} is used for internal debugging contexts.
   * @param materialize if {@code true}, materializes "lazy" content; if {@code false} "lazy"
   *                    content is not materialized
   * @return a {@code String} representation of this {@code Cell}
   */
  public final String toString(boolean materialize) {
    StringBuilder sb = new StringBuilder(128);
    sb.append("LazyCell[");
    if (!materialize) {
      sb.append(defIndex).append('/').append(valueIndex).append(' ');
    }
    sb.append("definition=").append((materialize ? definition() : def));
    sb.append(" value='").append((materialize ? value() : value)).append('\'');
    sb.append(']');
    return sb.toString();
  }
}
