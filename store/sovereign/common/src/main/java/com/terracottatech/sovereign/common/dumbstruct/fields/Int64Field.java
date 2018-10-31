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
public class Int64Field extends AbstractStructField {
  /**
   * Instantiates a new Int 64 field.
   *
   * @param offsetWithinStruct the offset
   * @param many               the many
   */
  public Int64Field(int offsetWithinStruct, int many) {
    super("F int64", offsetWithinStruct, many, 8);
  }

  public void put(Accessor a, long v) {
    a.getDataBuffer().putLong(address(a), v);
  }

  public long get(Accessor a) {
    return a.getDataBuffer().getLong(address(a));
  }

  public void put(Accessor a, int index, long v) {
    a.getDataBuffer().putLong(address(a, index), v);
  }

  public long get(Accessor a, int index) {
    return a.getDataBuffer().getLong(address(a, index));
  }
}
