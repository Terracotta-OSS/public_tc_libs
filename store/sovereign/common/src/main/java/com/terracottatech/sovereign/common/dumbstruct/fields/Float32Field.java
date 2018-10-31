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
public class Float32Field extends AbstractStructField {
  /**
   * Instantiates a new Float 32 field.
   *
   * @param offsetWithinStruct the offset
   * @param many               the many
   */
  public Float32Field(int offsetWithinStruct, int many) {
    super("F float32", offsetWithinStruct, many, 4);
  }

  public void put(Accessor a, float v) {
    a.getDataBuffer().putFloat(address(a), v);
  }

  public float get(Accessor a) {
    return a.getDataBuffer().getFloat(address(a));
  }

  public void put(Accessor a, int index, float v) {
    a.getDataBuffer().putFloat(address(a, index), v);
  }

  public float get(Accessor a, int index) {
    return a.getDataBuffer().getFloat(address(a, index));
  }
}
