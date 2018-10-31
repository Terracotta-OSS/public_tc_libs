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

import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;

public class ByteArrayFieldTest {

  @Test
  public void testNothing() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 10);
    ByteArrayField baf = new ByteArrayField(10, 100);
    Assert.assertThat(baf.getSingleFieldSize(), is(1));
    Assert.assertThat(baf.getAllocationCount(), is(100));
    Assert.assertThat(baf.getAllocatedSize(), is(100));
  }

  @Test
  public void testPutGetIndividualBytes() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    ByteArrayField baf = new ByteArrayField(10, 100);
    for (int i = 0; i < baf.getAllocationCount(); i++) {
      baf.put(access, i, (byte) i);
    }
    for (int i = 0; i < baf.getAllocationCount(); i++) {
      Assert.assertThat(baf.get(access, i), is((byte) i));
    }
  }

  @Test
  public void testPutGetByteBulk() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    ByteArrayField baf = new ByteArrayField(10, 100);
    byte[] b1 = new byte[]{0, 1, 2, 3};
    baf.put(access, 1, b1, 0, b1.length);
    for (int i = 0; i < b1.length; i++) {
      Assert.assertThat(baf.get(access, i + 1), is(((byte) (i))));
    }
    byte[] dest = new byte[3];
    baf.get(access, 2, dest, 0, dest.length);
    Assert.assertThat(dest[0], is((byte) 1));
    Assert.assertThat(dest[1], is((byte) 2));
    Assert.assertThat(dest[2], is((byte) 3));
  }

  @Test
  public void testMoveForward() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    ByteArrayField baf = new ByteArrayField(10, 100);
    for (int i = 0; i < baf.getAllocationCount(); i++) {
      baf.put(access, i, (byte) (i));
    }
    baf.move(access, 0, 1, baf.getAllocationCount() - 1);
    Assert.assertThat(baf.get(access, 0), is((byte) 0));
    for (int i = 0; i < baf.getAllocationCount() - 1; i++) {
      Assert.assertThat(baf.get(access, i + 1), is((byte) (i)));
    }
  }

  @Test
  public void testMoveBackward() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    ByteArrayField baf = new ByteArrayField(10, 100);
    for (int i = 0; i < baf.getAllocationCount(); i++) {
      baf.put(access, i, (byte) (i));
    }
    baf.move(access, 1, 0, baf.getAllocationCount() - 1);
    Assert.assertThat(baf.get(access, baf.getAllocationCount()-1), is((byte)(baf.getAllocationCount()-1)));
    for(int i=0;i<baf.getAllocationCount()-1;i++) {
      Assert.assertThat(baf.get(access, i), is((byte)(i+1)));
    }
  }
}
