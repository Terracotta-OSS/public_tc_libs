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

package com.terracottatech.sovereign.btrees.stores.disk;

import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;

import static com.terracottatech.sovereign.common.utils.FileUtils.fileChannel;
import static org.hamcrest.core.Is.is;

public class SimpleBlockBufferTest {

  public static final int BSIZE = 16 * 1024;
  private File file;
  private FileChannel fc;

  @Before
  public void before() throws IOException {
    file = File.createTempFile("foo1", "foo2");
    file.delete();
    this.fc = fileChannel(file);

  }

  @After
  public void after() throws IOException {
    fc.close();
    file.delete();
  }

  private void ensure(FileChannel fc, int i) throws IOException {
    fc.write(ByteBuffer.wrap(new byte[0]), i);
  }

  @Test
  public void testCreate() throws IOException {
    ensure(fc, BSIZE);

    BlockBuffer pb = new SimpleBlockBuffer(1,
      new SimpleBlockBuffer.Factory(PageSourceLocation.heap(), new DiskBufferProvider.Unmapped(), BSIZE), fc, 0);
    pb.flush();
  }

  @Test
  public void testFaultInRead() throws IOException {
    ensure(fc, BSIZE);
    BlockBuffer pb = new SimpleBlockBuffer(1,
      new SimpleBlockBuffer.Factory(PageSourceLocation.heap(), new DiskBufferProvider.Unmapped(), BSIZE), fc, 0);

    ByteBuffer buf = ByteBuffer.allocate(BSIZE);
    pb.read(0, buf);
    Assert.assertThat(buf.capacity(), is(BSIZE));
    for (int i = 0; i < buf.remaining(); i++) {
      Assert.assertThat(buf.get(i), is((byte) 0));
    }
  }

  @Test
  public void testWriteRead() throws IOException {
    ensure(fc, BSIZE);
    BlockBuffer pb = new SimpleBlockBuffer(1,
      new SimpleBlockBuffer.Factory(PageSourceLocation.heap(), new DiskBufferProvider.Unmapped(), BSIZE), fc, 0);
    ByteBuffer buf = ByteBuffer.allocate(BSIZE);
    Assert.assertThat(buf.capacity(), is(BSIZE));
    for (int i = 0; i < buf.remaining(); i++) {
      buf.put(i, (byte) i);
    }
    pb.write(0, buf);
    buf = ByteBuffer.allocate(BSIZE);
    pb.read(0, buf);
    for (int i = 0; i < buf.remaining(); i++) {
      Assert.assertThat(buf.get(i), is((byte) i));
    }
  }

  @Test
  public void testWriteReadFlush() throws IOException {
    ensure(fc, BSIZE);
    BlockBuffer pb = new SimpleBlockBuffer(1,
      new SimpleBlockBuffer.Factory(PageSourceLocation.heap(), new DiskBufferProvider.Unmapped(), BSIZE), fc, 0);
    ByteBuffer buf = ByteBuffer.allocate(BSIZE);
    Assert.assertThat(buf.capacity(), is(BSIZE));
    for (int i = 0; i < buf.remaining(); i++) {
      buf.put(i, (byte) i);
    }
    pb.write(0, buf);
    buf = ByteBuffer.allocate(BSIZE);
    pb.read(0, buf);
    for (int i = 0; i < buf.remaining(); i++) {
      Assert.assertThat(buf.get(i), is((byte) i));
    }
    pb.flush();
  }

  @Test
  public void testWriteFlushRead() throws IOException {
    ensure(fc, BSIZE);
    BlockBuffer pb = new SimpleBlockBuffer(1,
      new SimpleBlockBuffer.Factory(PageSourceLocation.heap(), new DiskBufferProvider.Unmapped(), BSIZE), fc, 0);
    ByteBuffer buf = ByteBuffer.allocate(BSIZE);
    Assert.assertThat(buf.capacity(), is(BSIZE));
    for (int i = 0; i < buf.remaining(); i++) {
      buf.put(i, (byte) i);
    }
    pb.write(0, buf);
    pb.flush();
    buf = ByteBuffer.allocate(BSIZE);
    pb.read(0, buf);
    for (int i = 0; i < buf.remaining(); i++) {
      Assert.assertThat(buf.get(i), is((byte) i));
    }
  }

  @Test
  public void testFreeAndAlloc() throws IOException {
    ensure(fc, 4096);
    BlockBuffer bb = new SimpleBlockBuffer(1,
      new SimpleBlockBuffer.Factory(PageSourceLocation.heap(), new DiskBufferProvider.Unmapped(), 4096), fc, 0);
    Assert.assertThat(bb.getFreePages(),is(0));
    bb.initAllocations(20);
    Assert.assertThat(bb.getFreePages(), is(0));
    HashSet<Integer> set=new HashSet<Integer>();
    for(int i=0;i<10;i++) {
      bb.free(i*40);
      set.add(i*40);
    }
    Assert.assertThat(bb.getFreePages(), is(10));
    for(int i=0;i<5;i++) {
      Assert.assertTrue(set.contains(bb.alloc()));
    }
    Assert.assertThat(bb.getFreePages(), is(5));
    bb.initAllocations(20);
    Assert.assertThat(bb.getFreePages(), is(0));
    for(int i=0;i<20;i++) {
      bb.free(i * 40);
    }
    try {
      bb.free(21 * 40);
      Assert.fail();
    } catch(Throwable t) {
    }
  }

}
