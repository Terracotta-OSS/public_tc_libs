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
package com.terracottatech.sovereign.common.dumbstruct;

import com.terracottatech.sovereign.common.dumbstruct.fields.Char16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Float32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Float64Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int64Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int8Field;

/**
 * This adds some syntactic sugar to the base builder. Primary class for building nested,
 * arrays of structs and fields.
 *
 * @author cschanck
 */
public class StructBuilder extends BaseStructBuilder {
  /**
   * Instantiates a new Nice struct builder.
   */
  public StructBuilder() {
    super();
  }

  /**
   * Lay out a single byte field.
   * @return Byte field handle.
   */
  public Int8Field int8() {
    return int8(1);
  }

  /**
   * Layout a single short field.
   * @return Short field handle.
   */
  public Int16Field int16() {
    return int16(1);
  }

  /**
   * Layout a single short field.
   * @return Short field handle.
   */
  public Int32Field int32() {
    return int32(1);
  }

  /**
   * Layout a single short field.
   * @return Short field handle.
   */
  public Char16Field char16() {
    return char16(1);
  }

  /**
   * Layout a single short field.
   * @return Short field handle.
   */
  public Float32Field float32() {
    return float32(1);
  }

  /**
   * Layout a single double field.
   * @return Double field handle.
   */
  public Float64Field float64() {
    return float64(1);
  }

  /**
   * Layout a single long field.
   * @return Long field handle.
   */
  public Int64Field int64() {
    return int64(1);
  }

  /**
   * End the current struct.
   * @return
   */
  public Struct end() {
    return end(1);
  }

}
