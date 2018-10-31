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
import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class SizedStringFieldImplsTest {

  private void probeIndexOOB(Accessor a, SizedStringField ss, char[] fail) {
    try {
      Arrays.fill(fail, 'a');
      ss.putString(a, new String(fail));
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }

  private void probeMultiPutGet(Accessor a, SizedStringField ss) {
    ss.putString(a, "hello1");
    Assert.assertThat(ss.getString(a), is("hello1"));
    ss.putString(a,  "fubaring1");
    Assert.assertThat(ss.getString(a), is("fubaring1"));
    ss.putString(a, "g1");
    Assert.assertThat(ss.getString(a), is("g1"));
  }

  @Test
  public void testSingleSmallStringSanity() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    AbstractSizedStringField.SmallStringField ss = new AbstractSizedStringField.SmallStringField(0, 20);
    Assert.assertThat(ss.getOffsetWithinStruct(), is(0));
    Assert.assertThat(ss.getAllocatedSize(), is(41));
    Assert.assertThat(ss.getSingleFieldSize(), is(41));
    Assert.assertThat(ss.getAllocationCount(), is(1));
    ss.putString(a, "hello");
    Assert.assertThat(ss.getString(a), is("hello"));
    Assert.assertThat(ss.getLength(a), is(5));
    probeIndexOOB(a, ss, new char[129]);
  }

  @Test
  public void testMultiSmallStringCreatePutGet() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    AbstractSizedStringField.SmallStringField ss = new AbstractSizedStringField.SmallStringField( 0, 20);
    probeMultiPutGet(a, ss);
  }

  @Test
  public void testSingleMediumStringSanity() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    AbstractSizedStringField.MediumStringField ss = new AbstractSizedStringField.MediumStringField( 0, 20);
    Assert.assertThat(ss.getOffsetWithinStruct(), is(0));
    Assert.assertThat(ss.getAllocatedSize(), is(42));
    Assert.assertThat(ss.getSingleFieldSize(), is(42));
    Assert.assertThat(ss.getAllocationCount(), is(1));
    ss.putString(a, "hello");
    Assert.assertThat(ss.getString(a), is("hello"));
    Assert.assertThat(ss.getLength(a), is(5));
    probeIndexOOB(a, ss, new char[Short.MAX_VALUE + 1]);
  }

  @Test
  public void testMultiMediumStringCreatePutGet() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    AbstractSizedStringField.MediumStringField ss = new AbstractSizedStringField.MediumStringField(0, 20);
    probeMultiPutGet(a, ss);
  }

  @Test
  public void testSingleLargeStringSanity() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    AbstractSizedStringField.LargeStringField ss = new AbstractSizedStringField.LargeStringField(0, 20);
    Assert.assertThat(ss.getOffsetWithinStruct(), is(0));
    Assert.assertThat(ss.getAllocatedSize(), is(44));
    Assert.assertThat(ss.getSingleFieldSize(), is(44));
    Assert.assertThat(ss.getAllocationCount(), is(1));
    ss.putString(a, "hello");
    Assert.assertThat(ss.getString(a), is("hello"));
    Assert.assertThat(ss.getLength(a), is(5));
  }

  @Test
  public void testMultiLargeStringCreatePutGet() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    AbstractSizedStringField.LargeStringField ss = new AbstractSizedStringField.LargeStringField( 0, 20);
    probeMultiPutGet(a, ss);
  }

}
