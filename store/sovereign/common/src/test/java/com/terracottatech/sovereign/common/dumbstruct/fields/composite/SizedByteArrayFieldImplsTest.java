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

public class SizedByteArrayFieldImplsTest {

  @Test
  public void testSmallByteArray() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    AbstractSizedByteArrayField.SmallByteArrayField ss = new AbstractSizedByteArrayField.SmallByteArrayField(0, 20);
    probeSanity(a, ss, 21);
    probeMultiGetPut(a, ss);
    probeIndexOutOfBounds(a, ss);
  }

  @Test
  public void testMediumByteArray() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    final int sz = Byte.MAX_VALUE + 1;
    AbstractSizedByteArrayField.MediumByteArrayField ss = new AbstractSizedByteArrayField.MediumByteArrayField(0, sz);
    probeSanity(a, ss, sz + 2);
    probeMultiGetPut(a, ss);
    probeIndexOutOfBounds(a, ss);
  }

  @Test
  public void testLargeByteArray() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    final int sz = Short.MAX_VALUE + 1;
    AbstractSizedByteArrayField.LargeByteArrayField ss = new AbstractSizedByteArrayField.LargeByteArrayField(0, sz);
    probeSanity(a, ss, sz + 4);
    probeMultiGetPut(a, ss);
  }

  private void probeSanity(Accessor a, SizedByteArrayField ss, int expectedSize) {
    Assert.assertThat(ss.getOffsetWithinStruct(), is(0));
    Assert.assertThat(ss.getAllocatedSize(), is(expectedSize));
    Assert.assertThat(ss.getSingleFieldSize(), is(expectedSize));
    Assert.assertThat(ss.getAllocationCount(), is(1));
  }

  private void probeSingleGetPut(Accessor a, SizedByteArrayField ss, int many) {
    byte[] b = new byte[many];
    Arrays.fill(b, (byte) 2);
    ss.putBytes(a, b);
    Assert.assertThat(ss.getBytes(a), is(b));
  }

  private void probeMultiGetPut(Accessor a, SizedByteArrayField ss) {
    probeSingleGetPut(a, ss, 4);
    probeSingleGetPut(a, ss, 8);
    probeSingleGetPut(a, ss, 1);
  }

  private void probeIndexOutOfBounds(Accessor a, SizedByteArrayField ss) {
    try {
      probeSingleGetPut(a, ss, ss.getAllocatedSize());
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }
}
