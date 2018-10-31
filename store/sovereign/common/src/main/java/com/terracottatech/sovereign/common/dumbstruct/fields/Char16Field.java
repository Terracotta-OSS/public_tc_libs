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
package com.terracottatech.sovereign.common.dumbstruct.fields;

import com.terracottatech.sovereign.common.dumbstruct.Accessor;

/**
 * @author cschanck
 */
public class Char16Field extends AbstractStructField {

  /**
   * Instantiates a new Char 16 field.
   *
   * @param offsetWithinStruct the offset
   * @param size               the size
   */
  public Char16Field(int offsetWithinStruct, int size) {
    super("F char16", offsetWithinStruct, size, 2);
  }

  public void put(Accessor a, char v) {
    a.getDataBuffer().putChar(address(a), v);
  }

  public char get(Accessor a) {
    return a.getDataBuffer().getChar(address(a));
  }

  public void put(Accessor a, int index, char v) {
    a.getDataBuffer().putChar(address(a, index), v);
  }

  public char get(Accessor a, int index) {
    return a.getDataBuffer().getChar(address(a, index));
  }
}
