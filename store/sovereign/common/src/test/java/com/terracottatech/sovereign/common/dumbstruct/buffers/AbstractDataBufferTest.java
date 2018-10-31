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

package com.terracottatech.sovereign.common.dumbstruct.buffers;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static java.util.Arrays.fill;
import static org.hamcrest.CoreMatchers.is;

public abstract class AbstractDataBufferTest {

  public abstract int max();

  public abstract DataBuffer make();

  @Test
  public void testCreate() {
    DataBuffer db = make();
    Assert.assertThat(db.size(), is(max()));
  }

  @Test
  public void testSingleByteOps() {
    DataBuffer db = make();
    for (int i = 0; i < max(); i++) {
      if(i==98) {
        int n=0;
      }
      db.put(i, (byte) i);
    }
    for (int i = 0; i < max(); i++) {
      Assert.assertThat(db.get(i), is((byte) i));
    }
    probeBounds(db, 1, max());
  }

  private void probeBounds(DataBuffer db, int width, int max) {
    probeBoundsGet(db, width, max);
    probeBoundsGet(db, width, -1);
    probeBoundsPut(db, width, max);
    probeBoundsPut(db, width, -1);
  }

  private void probeBoundsGet(DataBuffer db, int many, int offset) {
    try {
      switch (many) {
        case 1:
          db.get(offset);
          break;
        case 2:
          db.getShort(offset);
          break;
        case 4:
          db.getInt(offset);
          break;
        case 8:
          db.getLong(offset);
          break;
        default:
          throw new IllegalStateException();
      }
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
      // cool
    }
  }

  private void probeBoundsPut(DataBuffer db, int many, int offset) {
    try {
      switch (many) {
        case 1:
          db.put(offset, (byte) 1);
          break;
        case 2:
          db.putShort(offset, (short) 1);
          break;
        case 4:
          db.putInt(offset, 1);
          break;
        case 8:
          db.putLong(offset, 1L);
          break;
        default:
          throw new IllegalStateException();
      }
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
      // cool
    }
  }

  @Test
  public void testSingleShortOps() {
    DataBuffer db = make();
    for (int i = 0; i < max() / 2; i = i + 2) {
      db.putShort(i, (short) i);
    }
    for (int i = 0; i < max() / 2; i = i + 2) {
      Assert.assertThat(db.getShort(i), is((short) i));
    }
    probeBounds(db, 2, max());
  }

  @Test
  public void testSingleIntOps() {
    DataBuffer db = make();
    for (int i = 0; i < max() / 4; i = i + 4) {
      db.putInt(i, i);
    }
    for (int i = 0; i < max() / 4; i = i + 4) {
      Assert.assertThat(db.getInt(i), is(i));
    }
    probeBounds(db, 4, max());
  }

  @Test
  public void testSingleLongOps() {
    DataBuffer db = make();
    for (int i = 0; i < max() / 8; i = i + 8) {
      db.putLong(i, (long) i);
    }
    for (int i = 0; i < max() / 8; i = i + 8) {
      Assert.assertThat(db.getLong(i), is((long) i));
    }
    probeBounds(db, 8, max());
  }

  @Test
  public void testCopyWithinForward() {
    DataBuffer db = make();
    for (int i = 0; i < max(); i++) {
      db.put(i, (byte) i);
    }
    db.copyWithin(0, 1, max() - 1);
    for (int i = 1; i < max(); i++) {
      Assert.assertThat(db.get(i), is((byte) (i - 1)));
    }
    Assert.assertThat(db.get(0), is((byte) (0)));
  }

  @Test
  public void testCopyWithinBackward() {
    DataBuffer db = make();
    for (int i = 0; i < max(); i++) {
      db.put(i, (byte) i);
    }
    db.copyWithin(1, 0, max() - 1);
    for (int i = 0; i < max() - 1; i++) {
      Assert.assertThat(db.get(i), is((byte) (i + 1)));
    }
    Assert.assertThat(db.get(max() - 1), is((byte) (max() - 1)));
  }

  @Test
  public void testCopyToDataBufferFromByteArray() {
    DataBuffer db = make();
    byte[] barr = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    db.put(barr, 0, 9, 1);
    for (int i = 1; i < 10; i++) {
      Assert.assertThat(db.get(i), is((byte) i));
    }
    db.fill((byte) 0);
    db.put(barr, 7, 1, 0);
    Assert.assertThat(db.get(0), is((byte) 8));
  }

  @Test
  public void testCopyFromDataBufferToByteArray() {
    DataBuffer db = make();
    byte[] barr = new byte[10];
    for (int i = 0; i < db.size(); i++) {
      db.put(i, (byte) i);
    }
    db.get(4, barr, 0, barr.length);
    for (int i = 0; i < barr.length; i++) {
      Assert.assertThat(barr[i], is((byte) (i + 4)));
    }
    fill(barr, (byte) 0);
    db.get(4, barr, 2, 2);
    Assert.assertThat(barr[0], is((byte) 0));
    Assert.assertThat(barr[1], is((byte) 0));
    Assert.assertThat(barr[2], is((byte) 4));
    Assert.assertThat(barr[3], is((byte) 5));
    Assert.assertThat(barr[4], is((byte) 0));
    Assert.assertThat(barr[5], is((byte) 0));
    Assert.assertThat(barr[6], is((byte) 0));
    Assert.assertThat(barr[7], is((byte) 0));
    Assert.assertThat(barr[8], is((byte) 0));
    Assert.assertThat(barr[9], is((byte) 0));
  }

  @Test
  public void testCopyToDataBufferFromByteBuffer() {
    DataBuffer db = make();
    ByteBuffer b = ByteBuffer.allocate(9);
    for (int i = 0; i < b.capacity(); i++) {
      b.put(i, (byte) i);
    }
    b.clear();
    db.put(b, 1);
    Assert.assertThat(b.remaining(), is(0));
    for (int i = 1; i < 10; i++) {
      Assert.assertThat(db.get(i), is((byte) (i - 1)));
    }
  }

  @Test
  public void testCopyFromDataBufferToByteBuffer() {
    DataBuffer db = make();
    ByteBuffer b = ByteBuffer.allocate(10);
    for (int i = 0; i < db.size(); i++) {
      db.put(i, (byte) i);
    }
    db.get(4, b);
    Assert.assertThat(b.remaining(), is(0));
    for (int i = 0; i < b.capacity(); i++) {
      Assert.assertThat(b.get(i), is((byte) (i + 4)));
    }
    for (int i = 0; i < b.capacity(); i++) {
      b.put(i, (byte) 0);
    }
    b.clear();
    b.limit(2);
    db.get(4, b);
    Assert.assertThat(b.remaining(), is(0));
    b.clear();
    Assert.assertThat(b.get(0), is((byte) 4));
    Assert.assertThat(b.get(1), is((byte) 5));
    for (int i = 2; i < b.capacity(); i++) {
      Assert.assertThat(b.get(i), is((byte) 0));
    }
  }

  @Test
  public void testCopyDataBufferToDataBuffer() {
    DataBuffer db1 = make();
    DataBuffer db2 = make();
    for(int i=0;i<max()/2;i++) {
      db1.put(i,(byte)i);
    }
    db2.put(1,db1,0,max()/2);
    for(int i=0;i<max()/2;i++) {
      Assert.assertThat(db2.get(i+1),is((byte)i));
    }
  }
}
