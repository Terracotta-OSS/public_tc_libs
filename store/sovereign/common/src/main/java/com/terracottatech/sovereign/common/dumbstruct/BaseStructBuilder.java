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
import com.terracottatech.sovereign.common.dumbstruct.fields.StructField;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.ByteArrayField;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.SizedByteArrayField;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.AbstractSizedByteArrayField;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.SizedStringField;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.AbstractSizedStringField;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.StringField;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * This is the basic builder for complex structures. Better to use {@link com.terracottatech.sovereign.common.dumbstruct
 * .StructBuilder}.
 *
 * @author cschanck
 */
public class BaseStructBuilder {
  private List<StructField> current = new LinkedList<>();
  private final Deque<List<StructField>> parentStructs = new LinkedList<>();

  private int structOffset = 0;

  /**
   * Instantiates a new Base struct builder.
   */
  public BaseStructBuilder() {
  }

  private <T extends StructField> T record(T ret) {
    structOffset = structOffset + ret.getAllocatedSize();
    current.add(ret);
    return ret;
  }

  /**
   * Layout an arbitary number of byte fields.
   *
   * @param many how many
   * @return byte field
   */
  public Int8Field int8(int many) {
    return record(new Int8Field(structOffset, many));
  }

  /**
   * Layout an arbitary number of short fields.
   *
   * @param many how many
   * @return short field
   */
  public Int16Field int16(int many) {
    return record(new Int16Field(structOffset, many));
  }

  /**
   * Layout an arbitary number of int fields.
   *
   * @param many how many
   * @return int field
   */
  public Int32Field int32(int many) {
    return record(new Int32Field(structOffset, many));
  }

  /**
   * Layout an arbitary number of char fields.
   *
   * @param many how many
   * @return char field
   */
  public Char16Field char16(int many) {
    return record(new Char16Field(structOffset, many));
  }

  /**
   * Layout an arbitary number of float fields.
   *
   * @param many how many
   * @return float field
   */
  public Float32Field float32(int many) {
    return record(new Float32Field(structOffset, many));
  }

  /**
   * Layout an arbitary number of double fields.
   *
   * @param many how many
   * @return double field
   */
  public Float64Field float64(int many) {
    return record(new Float64Field(structOffset, many));
  }

  /**
   * Layout an arbitary number of long fields.
   *
   * @param many how many
   * @return long field
   */
  public Int64Field int64(int many) {
    return record(new Int64Field(structOffset, many));
  }

  /**
   * Add a fixed size byte[] field.
   *
   * @param maxSize size of the array
   * @return byte[] handle
   */
  public ByteArrayField bytes(int maxSize) {
    return record(new ByteArrayField(structOffset, maxSize));
  }

  /**
   * Add a fixed size String field.
   *
   * @param maxSize max size of the string.
   * @return string handle
   */
  public StringField string(int maxSize) {
    return record(new StringField(structOffset, maxSize));
  }

  /**
   * Start a new nested struct within the current one.
   */
  public void substruct() {
    parentStructs.push(current);
    current = new LinkedList<>();
  }

  /**
   * End the current struct, laying out a specific number of them.
   *
   * @param many number of structs in the array
   * @return handle to struct.
   */
  public Struct end(int many) {
    if (current.isEmpty()) {
      throw new IllegalStateException();
    }
    BasicStruct ret = new BasicStruct(current.get(0).getOffsetWithinStruct(), many, current);
    structOffset = structOffset + ret.getAllocatedSize();
    if (!parentStructs.isEmpty()) {
      current = parentStructs.pop();
      current.add(ret);
    }
    return ret;
  }

  /**
   * Record an array of sized strings,
   *
   * @param max max number of chars in a string, myst be less than or equal to Byte.MAX_VALUE
   * @return handle to 8 bit sized strings
   */
  public SizedStringField str8(int max) {
    return record(new AbstractSizedStringField.SmallStringField(structOffset, max));
  }

  /**
   * Record an array of sized strings,
   *
   * @param max max number of chars in a string, must be less than or equal to Short.MAX_VALUE
   * @return handle to 16 bit sized strings
   */
  public SizedStringField str16(int max) {
    return record(new AbstractSizedStringField.MediumStringField(structOffset, max));

  }

  /**
   * Record an array of sized strings,
   *
   * @param max max number of chars in a string, myst be less than or equal to Integer.MAX_VALUE
   * @return handle to 32 bit sized strings
   */
  public SizedStringField str32(int max) {
    return record(new AbstractSizedStringField.LargeStringField(structOffset, max));
  }

  /**
   * Record a sized byte array, with a max length of Byte.MAX_VAUE
   *
   * @param max
   * @return
   */
  public SizedByteArrayField bytes8(int max) {
    return record(new AbstractSizedByteArrayField.SmallByteArrayField(structOffset, max));
  }

  /**
   * Record a sized byte array, with a max length of Short.MAX_VAUE
   *
   * @param max
   * @return
   */
  public SizedByteArrayField bytes16(int max) {
    return record(new AbstractSizedByteArrayField.MediumByteArrayField(structOffset, max));
  }

  /**
   * Record a sized byte array, with a max length of Integer.MAX_VAUE
   *
   * @param max
   * @return
   */
  public SizedByteArrayField bytes32(int max) {
    return record(new AbstractSizedByteArrayField.LargeByteArrayField(structOffset, max));
  }

}
