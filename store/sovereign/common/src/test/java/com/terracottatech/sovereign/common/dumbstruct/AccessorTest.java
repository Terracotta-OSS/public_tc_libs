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

import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import com.terracottatech.sovereign.common.dumbstruct.fields.StructField;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AccessorTest {

  @Test
  public void testNothing() {
    DataBuffer db=new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor=new Accessor(db,0);
  }

  @Test
  public void testRightBuffer() {
    DataBuffer db=new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor=new Accessor(db,0);
    Assert.assertThat(accessor.getDataBuffer(),is(db));
  }

  @Test
  public void testRightOffset() {
    DataBuffer db=new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor=new Accessor(db,0);
    Assert.assertThat(accessor.getOffset(),is(0));
    accessor=new Accessor(db,10);
    Assert.assertThat(accessor.getOffset(),is(10));
  }

  @Test
  public void testRepoint() {
    DataBuffer db=new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor=new Accessor(db,0);
    Assert.assertThat(accessor.getOffset(),is(0));
    accessor=accessor.repoint(20);
    Assert.assertThat(accessor.getOffset(),is(20));
  }

  @Test
  public void testIncrementOffset() {
    DataBuffer db=new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor=new Accessor(db,0);
    accessor=accessor.increment(10);
    Assert.assertThat(accessor.getOffset(),is(10));
    StructField got = mock(StructField.class);
    when(got.getSingleFieldSize()).thenReturn(10);
    accessor=accessor.increment(got, 3);
    Assert.assertThat(accessor.getOffset(),is(40));
  }

  @Test
  public void testDecrementOffset() {
    DataBuffer db=new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor=new Accessor(db,100);
    accessor=accessor.decrement(10);
    Assert.assertThat(accessor.getOffset(),is(90));
    StructField got = mock(StructField.class);
    when(got.getSingleFieldSize()).thenReturn(10);
    accessor=accessor.decrement(got, 3);
    Assert.assertThat(accessor.getOffset(),is(60));
  }

}
