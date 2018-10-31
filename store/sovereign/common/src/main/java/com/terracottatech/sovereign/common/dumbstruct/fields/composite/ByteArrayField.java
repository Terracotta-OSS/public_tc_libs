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
public class ByteArrayField extends AbstractStructField {

  /**
   * Instantiates a new Byte array field.
   *
   * @param offsetWithinStruct the offset
   * @param size               the size
   */
  public ByteArrayField(int offsetWithinStruct, int size) {
    super("F byte[]", offsetWithinStruct, size, 1);
  }

  public byte get(Accessor a, int offset) {
    return a.getDataBuffer().get(address(a, offset));
  }

  public void put(Accessor a, int offset, byte val) {
    a.getDataBuffer().put(address(a, offset), val);
  }

  public void get(Accessor a, int offset, byte[] b, int start, int many) {
    a.getDataBuffer().get(address(a, offset), b, start, many);
  }

  public void put(Accessor a, int offset, byte[] b, int start, int many) {
    a.getDataBuffer().put(b, start, many, address(a, offset));
  }

  public void fill(Accessor a, byte value) {
    a.getDataBuffer().fill(address(a), getAllocatedSize(), value);
  }

}
