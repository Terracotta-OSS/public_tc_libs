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

import com.terracottatech.sovereign.common.dumbstruct.fields.composite.ByteArrayField;
import com.terracottatech.sovereign.common.dumbstruct.fields.Char16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Float32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Float64Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int64Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int8Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.StringField;
import com.terracottatech.sovereign.common.dumbstruct.fields.StructField;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class BaseStructBuilderTest {

  private void probePositions(Struct s) {
    int expected=s.getFields().get(0).getOffsetWithinStruct();
    for(StructField sf:s.getFields()) {
      Assert.assertThat(sf.getOffsetWithinStruct(),is(expected));
      expected=expected+sf.getAllocatedSize();
    }
  }
  private void fieldVarying(int many, StructField f1, Struct s, int size) {
    Assert.assertThat(s.getSingleFieldSize(), is(size * many));
    Assert.assertThat(s.getAllocatedSize(), is(many * size));
    Assert.assertThat(s.getAllocationCount(), is(1));
    Assert.assertThat(f1.getOffsetWithinStruct(), is(0));
    Assert.assertThat(s.getFields().size(), is(1));
  }

  private void structVarying(int many, StructField f1, Struct s, int size) {
    Assert.assertThat(s.getSingleFieldSize(), is(size));
    Assert.assertThat(s.getAllocatedSize(), is(many * size));
    Assert.assertThat(s.getAllocationCount(), is(many));
    Assert.assertThat(f1.getOffsetWithinStruct(), is(0));
    Assert.assertThat(s.getFields().size(), is(1));
  }

  @Test
  public void testInt8() throws Exception {
    i8VaryFieldCount(1);
    i8VaryFieldCount(4);
    i8VaryStructCount(1);
    i8VaryStructCount(4);
  }

  private void i8VaryFieldCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int8Field f1 = b.int8(many);
    Struct s = b.end(1);
    fieldVarying(many, f1, s, 1);
  }

  private void i8VaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int8Field f1 = b.int8(1);
    Struct s = b.end(many);
    structVarying(many, f1, s, 1);
  }

  @Test
  public void testInt16() throws Exception {
    i16VaryFieldCount(1);
    i16VaryFieldCount(4);
    i16VaryStructCount(1);
    i16VaryStructCount(4);
  }

  private void i16VaryFieldCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int16Field f1 = b.int16(many);
    Struct s = b.end(1);
    fieldVarying(many, f1, s, 2);
  }

  private void i16VaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int16Field f1 = b.int16(1);
    Struct s = b.end(many);
    structVarying(many, f1, s, 2);
  }

  @Test
  public void testInt32() throws Exception {
    i32VaryFieldCount(1);
    i32VaryFieldCount(4);
    i32VaryStructCount(1);
    i32VaryStructCount(4);
  }

  private void i32VaryFieldCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int32Field f1 = b.int32(many);
    Struct s = b.end(1);
    fieldVarying(many, f1, s, 4);
  }

  private void i32VaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int32Field f1 = b.int32(1);
    Struct s = b.end(many);
    structVarying(many, f1, s, 4);
  }

  @Test
  public void testInt64() throws Exception {
    i64VaryFieldCount(1);
    i64VaryFieldCount(4);
    i64VaryStructCount(1);
    i64VaryStructCount(4);
  }

  private void i64VaryFieldCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int64Field f1 = b.int64(many);
    Struct s = b.end(1);
    fieldVarying(many, f1, s, 8);
  }

  private void i64VaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Int64Field f1 = b.int64(1);
    Struct s = b.end(many);
    structVarying(many, f1, s, 8);
  }

  @Test
  public void testFloat32() throws Exception {
    f32VaryFieldCount(1);
    f32VaryFieldCount(4);
    f32VaryStructCount(1);
    f32VaryStructCount(4);
  }

  private void f32VaryFieldCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Float32Field f1 = b.float32(many);
    Struct s = b.end(1);
    fieldVarying(many, f1, s, 4);
  }

  private void f32VaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Float32Field f1 = b.float32(1);
    Struct s = b.end(many);
    structVarying(many, f1, s, 4);
  }

  @Test
  public void testFloat64() throws Exception {
    f64VaryFieldCount(1);
    f64VaryFieldCount(4);
    f64VaryStructCount(1);
    f64VaryStructCount(4);
  }

  private void f64VaryFieldCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Float64Field f1 = b.float64(many);
    Struct s = b.end(1);
    fieldVarying(many, f1, s, 8);
  }

  private void f64VaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    Float64Field f1 = b.float64(1);
    Struct s = b.end(many);
    structVarying(many, f1, s, 8);
  }

  @Test
  public void testByteArrayVaryStruct() {
    barrVaryStructCount(1);
    barrVaryStructCount(4);
  }

  private void barrVaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    ByteArrayField f1 = b.bytes(10);
    Struct s = b.end(many);
    structVarying(many, f1, s, 10);
  }

  @Test
  public void testStringVaryStruct() {
    stringVaryStructCount(1);
    stringVaryStructCount(4);
  }

  private void stringVaryStructCount(int many) {
    BaseStructBuilder b = new BaseStructBuilder();
    StringField f1 = b.string(10);
    Struct s = b.end(many);
    structVarying(many, f1, s, 20);
  }

  @Test
  public void testSingleStruct() {
    BaseStructBuilder b = new BaseStructBuilder();
    Int8Field f1 = b.int8(2);
    Char16Field f2 = b.char16(2);
    StringField f3 = b.string(100);
    Struct s1 = b.end(1);
    Assert.assertThat(s1.getAllocationCount(), is(1));
    Assert.assertThat(s1.getFields().size(), is(3));
    Assert.assertThat(s1.getSingleFieldSize(),
      is(f1.getAllocatedSize() + f2.getAllocatedSize() + f3.getAllocatedSize()));
    Assert.assertThat(s1.getFields(),contains((StructField)f1, (StructField)f2, (StructField)f3));
  }

  @Test
  public void testSubstruct() {
    BaseStructBuilder b = new BaseStructBuilder();
    Int8Field f1 = b.int8(2);
    Char16Field f2 = b.char16(2);
    StringField f3 = b.string(100);
    b.substruct();
    Int64Field f4=b.int64(2);
    Struct inner=b.end(1);
    Struct s1 = b.end(1);
    Assert.assertThat(s1.getAllocationCount(), is(1));
    Assert.assertThat(s1.getFields().size(), is(4));
    Assert.assertThat(s1.getSingleFieldSize(),
      is(f1.getAllocatedSize() + f2.getAllocatedSize() + f3.getAllocatedSize()+inner.getAllocatedSize()));
    Assert.assertThat(s1.getFields(),contains((StructField)f1, (StructField)f2, (StructField)f3, (StructField)inner));
    probePositions(inner);
    probePositions(s1);
  }
}
