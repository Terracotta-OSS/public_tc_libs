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

package com.terracottatech.sovereign.btrees.stores;

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

/**
 * @author cschanck
 */
public abstract class AbstractSimpleStoreTestBase {

  public abstract SimpleStore getSimpleStoreReaderWriter() throws IOException;

  @Test
  public void testOpenNoCommit() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    ByteBuffer b = ByteBuffer.allocate(10);
    b.clear();
    try {
      lob.getCommitData(b);
      fail();
    } catch (IOException e) {
      // Expected
    }
    lob.close();
  }

  @Test
  public void testCommitWithData() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();

    ByteBuffer b = ByteBuffer.allocate(8);
    b.asIntBuffer().put(0, 100);
    b.asIntBuffer().put(1, 200);
    b.clear();
    lob.commit(true, b);

    b = ByteBuffer.allocate(8);
    b.clear();
    lob.getCommitData(b);
    Assert.assertTrue(b.remaining() == 8);
    Assert.assertTrue(b.asIntBuffer().get(0) == 100);
    Assert.assertTrue(b.asIntBuffer().get(1) == 200);
    lob.close();
  }

  @Test
  public void testReadWriteNoCommit() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();

    ByteBuffer b1 = ByteBuffer.allocate(1024);
    b1.clear();
    fill(b1, 1);
    lob.append(b1);
    ByteBuffer cm = ByteBuffer.allocate(8);
    try {
      lob.getCommitData(cm);
      fail();
    } catch (IOException e) {
      // Expected
    }

    lob.close();
  }

  @Test
  public void testReadWriteCommit() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();

    ByteBuffer b1 = ByteBuffer.allocate(1024);
    b1.clear();
    fill(b1, 1);
    long got = lob.append(b1);
    ByteBuffer cm = ByteBuffer.allocate(8);
    cm.clear();
    cm.putLong(0, got);
    lob.commit(true, cm);

    ByteBuffer cm1 = ByteBuffer.allocate(8);
    lob.getCommitData(cm1);
    Assert.assertTrue(cm1.asLongBuffer().get(0) == got);

    lob.close();
  }

  @Test
  public void testReadWriteData() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();

    ByteBuffer b1 = ByteBuffer.allocate(1024);
    b1.clear();
    fill(b1, 1);
    long got = lob.append(b1);
    ByteBuffer cm = ByteBuffer.allocate(8);
    cm.clear();
    cm.putLong(0, got);
    lob.commit(true, cm);

    b1.clear();
    fill(b1, 0);
    lob.read(got, b1);
    IntBuffer ib = b1.asIntBuffer().slice();
    for (int i = 0; i < ib.capacity(); i++) {
      Assert.assertThat(ib.get(i), is(1));
    }
    lob.close();
  }

  private void fill(ByteBuffer b1, int i) {
    IntBuffer tmp = b1.duplicate().asIntBuffer();
    while (tmp.hasRemaining()) {
      tmp.put(i);
    }
  }

  @Test
  public void testReclaiming() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    long old = lob.append(ByteBuffer.allocate(10));
    lob.storeFree(old, false);
    lob.commit(true, ByteBuffer.allocate(10));
    long old2 = lob.append(ByteBuffer.allocate(10));
    Assert.assertThat(old, is(old2));
    lob.close();
  }

  // scatter write tests
  // single buffer > pagesize
  // multiple buffer, first larger > pagesize
  // multiple buffer, second larger > pagesize
  // single buffer == pagesize
  // multiple buffer, first == pagesize
  // multiple buffer total == pagesize
  // multiple buffer total < pagesize
  // multiple buffer, both > pagesize

  @Test
  public void testScatterWriterSingleSmallBuffer() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    ByteBuffer in = ByteBuffer.allocate(pgsize - 1);
    NIOBufferUtils.fill(in, (byte) 1);
    long addr = lob.append(in);
    ByteBuffer out = ByteBuffer.allocate(pgsize - 1);
    lob.read(addr, out);
    out.clear();
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is((byte) 1));
    }
    System.out.println(out);
  }

  @Test
  public void testScatterWriterOneBigOneSmall() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    ByteBuffer in1 = ByteBuffer.allocate(pgsize + 1);
    ByteBuffer in2 = ByteBuffer.allocate(24);
    NIOBufferUtils.fill(in1, (byte) 1);
    NIOBufferUtils.fill(in2, (byte) 2);
    long addr = lob.append(in1, in2);
    ByteBuffer out = ByteBuffer.allocate(in1.capacity() + in2.capacity());
    lob.read(addr, out);
    out.clear();
    int cnt = 0;
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is((cnt++ < in1.capacity()) ? ((byte) 1) : (byte) 2));
    }
  }

  @Test
  public void testScatterWriterOneSmallOneBig() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    ByteBuffer in1 = ByteBuffer.allocate(24);
    ByteBuffer in2 = ByteBuffer.allocate(pgsize + 1);
    NIOBufferUtils.fill(in1, (byte) 1);
    NIOBufferUtils.fill(in2, (byte) 2);
    long addr = lob.append(in1, in2);
    ByteBuffer out = ByteBuffer.allocate(in1.capacity() + in2.capacity());
    lob.read(addr, out);
    out.clear();
    int cnt = 0;
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is((cnt++ < in1.capacity()) ? ((byte) 1) : (byte) 2));
    }
    System.out.println(out);
  }

  @Test
  public void testScatterWriterOneExactBuffer() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    ByteBuffer in1 = ByteBuffer.allocate(pgsize);
    NIOBufferUtils.fill(in1, (byte) 1);
    long addr = lob.append(in1);
    ByteBuffer out = ByteBuffer.allocate(in1.capacity());
    lob.read(addr, out);
    out.clear();
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is(((byte) 1)));
    }
    System.out.println(out);
  }

  @Test
  public void testScatterWriterOneExactOneTrail() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    ByteBuffer in2 = ByteBuffer.allocate(pgsize);
    ByteBuffer in1 = ByteBuffer.allocate(24);
    NIOBufferUtils.fill(in1, (byte) 1);
    NIOBufferUtils.fill(in2, (byte) 2);
    long addr = lob.append(in1, in2);
    ByteBuffer out = ByteBuffer.allocate(in1.capacity() + in2.capacity());
    lob.read(addr, out);
    out.clear();
    int cnt = 0;
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is((cnt++ < in1.capacity()) ? ((byte) 1) : (byte) 2));
    }
    System.out.println(out);
  }

  @Test
  public void testScatterWriterTwoTotalExactPagesize() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    ByteBuffer in2 = ByteBuffer.allocate(pgsize-10);
    ByteBuffer in1 = ByteBuffer.allocate(10);
    NIOBufferUtils.fill(in1, (byte) 1);
    NIOBufferUtils.fill(in2, (byte) 2);
    long addr = lob.append(in1, in2);
    ByteBuffer out = ByteBuffer.allocate(in1.capacity() + in2.capacity());
    lob.read(addr, out);
    out.clear();
    int cnt = 0;
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is((cnt++ < in1.capacity()) ? ((byte) 1) : (byte) 2));
    }
    System.out.println(out);
  }

  @Test
  public void testScatterWriterTwoTotalLessThanPagesize() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    Assert.assertThat(pgsize,greaterThan(20));
    ByteBuffer in2 = ByteBuffer.allocate(10);
    ByteBuffer in1 = ByteBuffer.allocate(10);
    NIOBufferUtils.fill(in1, (byte) 1);
    NIOBufferUtils.fill(in2, (byte) 2);
    long addr = lob.append(in1, in2);
    ByteBuffer out = ByteBuffer.allocate(in1.capacity() + in2.capacity());
    lob.read(addr, out);
    out.clear();
    int cnt = 0;
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is((cnt++ < in1.capacity()) ? ((byte) 1) : (byte) 2));
    }
    System.out.println(out);
  }

  @Test
  public void testScatterWriterTwoBothBiggerThanPagesize() throws IOException {
    SimpleStore lob = getSimpleStoreReaderWriter();
    int pgsize = lob.getPageSize();
    ByteBuffer in2 = ByteBuffer.allocate(pgsize+1);
    ByteBuffer in1 = ByteBuffer.allocate(pgsize+1);
    NIOBufferUtils.fill(in1, (byte) 1);
    NIOBufferUtils.fill(in2, (byte) 2);
    long addr = lob.append(in1, in2);
    ByteBuffer out = ByteBuffer.allocate(in1.capacity() + in2.capacity());
    lob.read(addr, out);
    out.clear();
    int cnt = 0;
    while (out.hasRemaining()) {
      Assert.assertThat(out.get(), is((cnt++ < in1.capacity()) ? ((byte) 1) : (byte) 2));
    }
    System.out.println(out);
  }

  @Test
  public void testClosedAppend() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    try {
      lob.append(ByteBuffer.allocate(1024));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testClosedClose() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    lob.close();    // Second close should not fail
  }

  @Test
  public void testClosedCommit() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    try {
      lob.commit(true, ByteBuffer.allocate(1024));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testClosedDiscard() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    try {
      lob.discard();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testClosedGetCommitData() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    try {
      lob.getCommitData(ByteBuffer.allocate(1024));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testClosedGetPageSize() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    lob.getPageSize();
  }

  @Test
  public void testClosedGetStats() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    lob.getStats();
  }

  @Test
  public void testClosedGetStoreLocation() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    lob.getStoreLocation();
  }

  @Test
  public void testClosedIsNew() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    lob.isNew();
  }

  @Test
  public void testClosedRead() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    try {
      lob.read(0, ByteBuffer.allocate(1024));
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testClosedReadOnly() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    try {
      lob.readOnly(0, 1024);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testClosedStoreFree() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    try {
      lob.storeFree(0, true);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testClosedSupportsDurability() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    lob.supportsDurability();
  }

  @Test
  public void testClosedSupportsReadOnly() throws Exception {
    SimpleStore lob = getSimpleStoreReaderWriter();
    lob.close();

    lob.supportsReadOnly();
  }
}
