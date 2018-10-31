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

public class Int16FieldTest {

  @Test
  public void testInt16() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor = new Accessor(db, 0);
    Int16Field bf = new Int16Field(10, 1);
    Assert.assertThat(bf.getAllocatedSize(), is(2));
    Assert.assertThat(bf.getAllocationCount(), is(1));
    Assert.assertThat(bf.getSingleFieldSize(), is(2));

    bf.put(accessor, (short) 1);
    Assert.assertThat(bf.get(accessor), is((short) 1));
    bf.put(accessor, (short) 11);
    Assert.assertThat(bf.get(accessor), is((short) 11));
  }

  @Test
  public void testInt16Array() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor accessor = new Accessor(db, 0);
    Int16Field bf = new Int16Field(10, 6);
    Assert.assertThat(bf.getAllocatedSize(), is(6 * 2));
    Assert.assertThat(bf.getAllocationCount(), is(6));
    Assert.assertThat(bf.getSingleFieldSize(), is(2));

    bf.put(accessor, (short) 1);
    Assert.assertThat(bf.get(accessor), is((short) 1));
    for (int i = 0; i < bf.getAllocationCount(); i++) {
      bf.put(accessor, i, (short) i);
    }
    for (int i = 0; i < bf.getAllocationCount(); i++) {
      Assert.assertThat(bf.get(accessor, i), is((short) i));
    }

    // test move
    for (int i = 0; i < bf.getAllocationCount(); i++) {
      bf.put(accessor, i, (short) i);
    }
    bf.move(accessor, 0, 1, bf.getAllocationCount() - 1);
    Assert.assertThat(bf.get(accessor, 0), is((short) (0)));
    for (int i = 1; i < bf.getAllocationCount(); i++) {
      Assert.assertThat(bf.get(accessor, i), is((short) (i - 1)));
    }
    bf.move(accessor, 1, 0, bf.getAllocationCount() - 1);
    Assert.assertThat(bf.get(accessor, bf.getAllocationCount() - 1), is((short) (bf.getAllocationCount() - 1 - 1)));
    for (int i = 0; i < bf.getAllocationCount()-1; i++) {
      Assert.assertThat(bf.get(accessor, i), is((short) (i)));
    }
  }

}
