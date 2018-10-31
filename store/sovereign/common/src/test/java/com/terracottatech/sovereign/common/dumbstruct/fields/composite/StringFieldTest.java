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

public class StringFieldTest {

  @Test
  public void testNothing() {
    StringField csf = new StringField(0, 100);
    Assert.assertThat(csf.getSingleFieldSize(), is(2));
    Assert.assertThat(csf.getAllocationCount(), is(100));
    Assert.assertThat(csf.getAllocatedSize(), is(200));
  }

  @Test
  public void testPutGetString() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 10);
    StringField csf = new StringField(1, 100);
    csf.putString(access, "foobar");
    Assert.assertThat(csf.getString(access, 6), is("foobar"));
  }

  @Test
  public void testPutGetIndividualChars() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    StringField csf = new StringField(1, 10);
    for (int i = 0; i < csf.getAllocationCount(); i++) {
      csf.put(access, i, 'a');
    }
    for (int i = 0; i < csf.getAllocationCount(); i++) {
      Assert.assertThat(csf.get(access, i), is('a'));
    }
  }

  @Test
  public void testPutGetCharBulk() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    StringField csf = new StringField(1, 10);
    char[] c1 = new char[]{'a', 'b', 'c', 'd'};
    csf.put(access, 1, new String(c1), 0, c1.length);
    for (int i = 0; i < c1.length; i++) {
      Assert.assertThat(csf.get(access, i + 1), is(((char) ('a' + i))));
    }
    char[] dest = new char[3];
    csf.get(access, 2, dest, 0, dest.length);
    Assert.assertThat(dest[0], is('b'));
    Assert.assertThat(dest[1], is('c'));
    Assert.assertThat(dest[2], is('d'));
  }

  @Test
  public void testMoveForward() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    StringField csf = new StringField(1, 10);
    for (int i = 0; i < csf.getAllocationCount(); i++) {
      csf.put(access, i, (char) ('a' + i));
    }
    csf.move(access, 0, 1, csf.getAllocationCount() - 1);
    Assert.assertThat(csf.get(access, 0), is('a'));
    for(int i=0;i<csf.getAllocationCount()-1;i++) {
      Assert.assertThat(csf.get(access, i+1), is((char)('a'+i)));
    }
  }

  @Test
  public void testMoveBackward() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor access = new Accessor(db, 1);
    StringField csf = new StringField(1, 10);
    for (int i = 0; i < csf.getAllocationCount(); i++) {
      csf.put(access, i, (char) ('a' + i));
    }
    csf.move(access, 1, 0, csf.getAllocationCount() - 1);
    Assert.assertThat(csf.get(access, csf.getAllocationCount()-1), is((char)('a'+csf.getAllocationCount()-1)));
    for(int i=0;i<csf.getAllocationCount()-1;i++) {
      Assert.assertThat(csf.get(access, i), is((char)('a'+i+1)));
    }
  }
}
