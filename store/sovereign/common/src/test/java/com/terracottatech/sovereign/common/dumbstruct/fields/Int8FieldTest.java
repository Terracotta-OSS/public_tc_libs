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


import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;

public class Int8FieldTest {

  @Test
  public void testInt8() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor = new Accessor(db, 0);
    Int8Field bf = new Int8Field(10, 1);
    Assert.assertThat(bf.getAllocatedSize(), is(1));
    Assert.assertThat(bf.getAllocationCount(), is(1));
    Assert.assertThat(bf.getSingleFieldSize(), is(1));

    bf.put(accessor, (byte) 1);
    Assert.assertThat(bf.get(accessor), is((byte) 1));
    bf.put(accessor, (byte) 11);
    Assert.assertThat(bf.get(accessor), is((byte) 11));
    bf.fill(accessor, 0, 1, (byte) 14);
    Assert.assertThat(bf.get(accessor), is((byte) 14));
  }

  @Test
  public void testInt8Array() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor = new Accessor(db, 0);
    Int8Field bf = new Int8Field(10, 6);
    Assert.assertThat(bf.getAllocatedSize(), is(6));
    Assert.assertThat(bf.getAllocationCount(), is(6));
    Assert.assertThat(bf.getSingleFieldSize(), is(1));

    bf.put(accessor, (byte) 1);
    Assert.assertThat(bf.get(accessor), is((byte) 1));
    for (int i = 0; i < bf.getAllocationCount(); i++) {
      bf.put(accessor, i, (byte) i);
    }
    for (int i = 0; i < bf.getAllocationCount(); i++) {
      Assert.assertThat(bf.get(accessor, i), is((byte) i));
    }
    bf.fill(accessor, 0, 6, (byte) 22);
    for (int i = 0; i < bf.getAllocationCount(); i++) {
      Assert.assertThat(bf.get(accessor, i), is((byte) 22));
    }

    // test move
    for (int i = 0; i < bf.getAllocationCount(); i++) {
      bf.put(accessor, i, (byte) i);
    }
    bf.move(accessor, 0, 1, bf.getAllocationCount() - 1);
    Assert.assertThat(bf.get(accessor, 0), is((byte) (0)));
    for (int i = 1; i < bf.getAllocationCount(); i++) {
      Assert.assertThat(bf.get(accessor, i), is((byte) (i - 1)));
    }
    bf.move(accessor, 1, 0, bf.getAllocationCount() - 1);
    Assert.assertThat(bf.get(accessor, bf.getAllocationCount() - 1), is((byte) (bf.getAllocationCount() - 1 - 1)));
    for (int i = 0; i < bf.getAllocationCount()-1; i++) {
      Assert.assertThat(bf.get(accessor, i), is((byte) (i)));
    }
  }
}
