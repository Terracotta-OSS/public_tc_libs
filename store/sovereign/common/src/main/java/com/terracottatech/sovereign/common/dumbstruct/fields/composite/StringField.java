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
package com.terracottatech.sovereign.common.dumbstruct.fields.composite;

import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import com.terracottatech.sovereign.common.dumbstruct.fields.AbstractStructField;

/**
 * @author cschanck
 */
public class StringField extends AbstractStructField {
  /**
   * Instantiates a new String field.
   *
   * @param offsetWithinStruct the offset
   * @param size               the size
   */
  public StringField(int offsetWithinStruct, int size) {
    super("F string", offsetWithinStruct, size, 2);
  }

  public char get(Accessor a, int index) {
    return a.getDataBuffer().getChar(address(a, index));
  }

  public void put(Accessor a, int index, char val) {
    a.getDataBuffer().putChar(address(a, index), val);
  }

  public void get(Accessor a, int index, char[] b, int start, int many) {
    a.getDataBuffer().getString(address(a, index), b, start, many);
  }

  public String getString(Accessor a, int many) {
    char[] c = new char[many];
    a.getDataBuffer().getString(address(a), c, 0, many);
    return String.valueOf(c);
  }

  public String getString(Accessor a, int index, int many) {
    char[] c = new char[many];
    a.getDataBuffer().getString(address(a, index), c, 0, many);
    return String.valueOf(c);
  }

  public void put(Accessor a, int index, CharSequence src, int start, int many) {
    a.getDataBuffer().putString(src, start, many, address(a, index));
  }

  public void putString(Accessor a, CharSequence src) {
    a.getDataBuffer().putString(src, 0, src.length(), address(a));
  }

  public void putString(Accessor a, int index, CharSequence src) {
    a.getDataBuffer().putString(src, 0, src.length(), address(a, index));
  }

}
