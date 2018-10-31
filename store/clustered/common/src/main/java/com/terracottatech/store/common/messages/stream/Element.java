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

package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.Cell;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.common.messages.RecordData;

import java.util.Optional;

public class Element {

  private final ElementType type;
  private final Object data;

  public Element(ElementType type, Object data) {
    this.type = type;
    this.data = data;
  }

  public ElementType getType() {
    return type;
  }

  public Object getData() {
    return data;
  }

  /**
   * Gets the payload as a {@link RecordData} instance.
   * @return the payload cast as a {@code RecordData} instance
   * @throws ClassCastException if the payload is not {@code RecordData}
   */
  @SuppressWarnings("unchecked")
  public <K extends Comparable<K>> RecordData<K> getRecordData() {
    return (RecordData<K>)RecordData.class.cast(data);   // unchecked
  }

  /**
   * Gets the payload as a {@code double} value.
   * @return the payload as a {@code double}
   * @throws ClassCastException if the payload is not a {@code double}
   */
  public double getDoubleValue() {
    return Double.class.cast(data);
  }

  /**
   * Gets the payload as a {@code int} value.
   * @return the payload as a {@code int}
   * @throws ClassCastException if the payload is not an {@code int}
   */
  public int getIntValue() {
    return Integer.class.cast(data);
  }

  /**
   * Gets the payload as a {@code long} value.
   * @return the payload as a {@code long}
   * @throws ClassCastException if the payload is not a {@code long}
   */
  public long getLongValue() {
    return Long.class.cast(data);
  }

  /**
   * Gets the payload as an {@link ElementValue} instance.  The returned instance contains a
   * {@link java.util.stream.Stream Stream} element value that may be one if the following types:
   * <ul>
   *   <li>{@link Cell} value (or its {@link Optional})</li>
   *   <li>{@link CellSet} (or its {@code Optional})</li>
   * </ul>
   *
   * @return the {@code ElementValue} instance
   *
   * @throws ClassCastException if the payload is not a {@code Stream} element value
   */
  public ElementValue getElementValue() {
    return ElementValue.class.cast(data);
  }

}
