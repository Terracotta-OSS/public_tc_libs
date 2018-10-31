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

package com.terracottatech.sovereign.btrees.bplustree.appendonly;

import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import com.terracottatech.sovereign.common.dumbstruct.Accessor;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;

public class ANodeProxyTest {

  @Test
  public void testSetGetSizeLeaf() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.leaf();
    n.setHeader(a, 10);
    Assert.assertThat(ANodeProxy.fetchLeaf(a), is(true));
    Assert.assertThat(n.getSize(a), is(10));
  }

  @Test
  public void testSetGetSizeBranch() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.branch();
    n.setHeader(a, 10);
    Assert.assertThat(ANodeProxy.fetchLeaf(a), is(false));
    Assert.assertThat(n.getSize(a), is(10));
  }

  @Test
  public void testGetSetKeyLeaf() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.leaf();
    n.setKey(a, 0, 1l);
    n.setKey(a, 1, 2l);
    Assert.assertThat(n.getKey(a, 0), is(1l));
    Assert.assertThat(n.getKey(a, 1), is(2l));
  }

  @Test
  public void testGetSetKeyBranch() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.branch();
    n.setKey(a, 0, 1l);
    n.setKey(a, 1, 2l);
    Assert.assertThat(n.getKey(a, 0), is(1l));
    Assert.assertThat(n.getKey(a, 1), is(2l));
  }

  @Test
  public void testGetSetValueLeaf() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.leaf();
    n.setValue(a, 0, 11l);
    n.setValue(a, 1, 12l);
    Assert.assertThat(n.getValue(a, 0), is(11l));
    Assert.assertThat(n.getValue(a, 1), is(12l));
  }

  @Test
  public void testGetSetPointerBranch() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.branch();
    n.setPointer(a, 0, 21l);
    n.setPointer(a, 1, 22l);
    Assert.assertThat(n.getPointer(a, 0), is(21l));
    Assert.assertThat(n.getPointer(a, 1), is(22l));
  }

  @Test
  public void testInsertPointerKeyAt() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.branch();
    int sz = 0;
    n.insertPointerKeyAt(a, 0, 1l, 11l, sz++);
    n.insertPointerKeyAt(a, 1, 2l, 22l, sz++);
    n.insertPointerKeyAt(a, 2, 3l, 33l, sz++);
    for (int i = 0; i < 3; i++) {
      Assert.assertThat(n.getPointer(a, i), is((long) (i + 1)));
      Assert.assertThat(n.getKey(a, i), is((long) (11 * (i + 1))));
    }
    n.insertPointerKeyAt(a, 1, 100l, 101l, sz++);
    Assert.assertThat(n.getPointer(a, 0), is(1l));
    Assert.assertThat(n.getKey(a, 0), is(11l));
    Assert.assertThat(n.getPointer(a, 1), is(100l));
    Assert.assertThat(n.getKey(a, 1), is(101l));
    Assert.assertThat(n.getPointer(a, 2), is(2l));
    Assert.assertThat(n.getKey(a, 2), is(22l));
  }

  @Test
  public void testInsertKeyPointerAt() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 0);
    ANodeProxy n = ANodeProxy.branch();
    int sz = 0;
    n.setPointer(a, 0, 10);
    n.insertKeyPointerAt(a, 0, 1l, 11l, sz++);
    n.insertKeyPointerAt(a, 1, 2l, 22l, sz++);
    n.insertKeyPointerAt(a, 2, 3l, 33l, sz++);
    System.out.println(n.toString(a, sz));
    for (int i = 0; i < 3; i++) {
      Assert.assertThat(n.getKey(a, i), is((long) (i + 1)));
      Assert.assertThat(n.getPointer(a, i + 1), is((long) (11 * (i + 1))));
    }
    n.insertKeyPointerAt(a, 1, 50l, 51l, sz++);
    Assert.assertThat(n.getKey(a, 0), is(1l));
    Assert.assertThat(n.getKey(a, 1), is(50l));
    Assert.assertThat(n.getKey(a, 2), is(2l));
    Assert.assertThat(n.getPointer(a, 0), is(10l));
    Assert.assertThat(n.getPointer(a, 1), is(11l));
    Assert.assertThat(n.getPointer(a, 2), is(51l));
    Assert.assertThat(n.getPointer(a, 3), is(22l));

  }

  @Test
  public void testInsertKeyValueAt() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 0);
    ANodeProxy n = ANodeProxy.leaf();
    int sz = 0;
    n.insertKeyValueAt(a, 0, 1l, 11l, sz++);
    n.insertKeyValueAt(a, 1, 2l, 22l, sz++);
    n.insertKeyValueAt(a, 2, 3l, 33l, sz++);
    System.out.println(n.toString(a, sz));
    for (int i = 0; i < 3; i++) {
      Assert.assertThat(n.getKey(a, i), is((long) (i + 1)));
      Assert.assertThat(n.getValue(a, i), is((long) (11 * (i + 1))));
    }
    n.insertKeyValueAt(a, 1, 51l, 52l, sz++);
    System.out.println(n.toString(a, sz));
    Assert.assertThat(n.getKey(a, 0), is(1l));
    Assert.assertThat(n.getKey(a, 1), is(51l));
    Assert.assertThat(n.getKey(a, 2), is(2l));
    Assert.assertThat(n.getKey(a, 3), is(3l));

    Assert.assertThat(n.getValue(a, 0), is(11l));
    Assert.assertThat(n.getValue(a, 1), is(52l));
    Assert.assertThat(n.getValue(a, 2), is(22l));
    Assert.assertThat(n.getValue(a, 3), is(33l));

  }

  @Test
  public void testIsLeaf() throws Exception {
    Assert.assertThat(ANodeProxy.leaf().isLeaf(), is(true));
    Assert.assertThat(ANodeProxy.branch().isLeaf(), is(false));
  }

  @Test
  public void testGetSetRevision() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 10);
    ANodeProxy n = ANodeProxy.leaf();
    n.setRevision(a, 100l);
    Assert.assertThat(n.getRevision(a), is(100l));
    n = ANodeProxy.branch();
    n.setRevision(a, 101l);
    Assert.assertThat(n.getRevision(a), is(101l));
  }

  @Test
  public void testSizeFor() throws Exception {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 0);
    ANodeProxy n = ANodeProxy.leaf();
    n.setHeader(a, 0);
    Assert.assertThat(ANodeProxy.storageFor(a), is(n));
    Assert.assertThat(n.isLeaf(), is(true));
    n = ANodeProxy.branch();
    n.setHeader(a, 0);
    Assert.assertThat(n.isLeaf(), is(false));
    Assert.assertThat(ANodeProxy.storageFor(a), is(n));
  }

  @Test
  public void testRemoveKeyPointerAt() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 0);
    ANodeProxy n = ANodeProxy.branch();
    int sz = 0;
    n.setPointer(a, 0, -1l);
    n.insertKeyPointerAt(a, 0, 1l, 10l, sz++);
    n.insertKeyPointerAt(a, 1, 2l, 20l, sz++);
    n.insertKeyPointerAt(a, 2, 3l, 30l, sz++);
    n.removeKeyPointerAt(a,1,sz--);
    Assert.assertThat(n.getKey(a,0),is(1l));
    Assert.assertThat(n.getKey(a,1),is(3l));
    Assert.assertThat(n.getPointer(a,1),is(10l));
    Assert.assertThat(n.getPointer(a,2),is(30l));
  }

  @Test
  public void testRemovePointerKeyAt() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 0);
    ANodeProxy n = ANodeProxy.branch();
    int sz = 0;
    n.setPointer(a, 0, -1l);
    n.insertKeyPointerAt(a, 0, 1l, 10l, sz++);
    n.insertKeyPointerAt(a, 1, 2l, 20l, sz++);
    n.insertKeyPointerAt(a, 2, 3l, 30l, sz++);
    n.removePointerKeyAt(a,1,sz--);
    Assert.assertThat(n.getKey(a,0),is(1l));
    Assert.assertThat(n.getKey(a,1),is(3l));
    Assert.assertThat(n.getPointer(a,1),is(20l));
    Assert.assertThat(n.getPointer(a,2),is(30l));
  }

  @Test
  public void testRemoveKeyValueAt() {
    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db, 0);
    ANodeProxy n = ANodeProxy.leaf();
    int sz = 0;
    n.insertKeyValueAt(a, 0, 1l, 10l, sz++);
    n.insertKeyValueAt(a, 1, 2l, 20l, sz++);
    n.insertKeyValueAt(a, 2, 3l, 30l, sz++);
    n.removeKeyValueAt(a,1,sz--);
    Assert.assertThat(n.getKey(a,0),is(1l));
    Assert.assertThat(n.getKey(a,1),is(3l));
    Assert.assertThat(n.getValue(a,0),is(10l));
    Assert.assertThat(n.getValue(a,1),is(30l));
  }
}
