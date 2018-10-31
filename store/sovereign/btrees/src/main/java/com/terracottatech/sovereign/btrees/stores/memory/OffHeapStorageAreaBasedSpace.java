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
package com.terracottatech.sovereign.btrees.stores.memory;

import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.StoreOutOfSpaceException;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.location.StoreLocation;
import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.common.utils.TimedWatcher;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.PointerSize;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TimerTask;

/**
 * This is a simple store over the standard off heap allocator; tended not to work so well
 * given the fixed block size allocations the btree uses. Allocation costs were brutal.
 *
 * @author cschanck
 */
public class OffHeapStorageAreaBasedSpace implements SimpleStore {

  private final int pgSize;
  private final PageSourceLocation storeLocation;
  private volatile OffHeapStorageArea offheap;
  private volatile long highWaterMark = 0;
  private ByteBuffer commitBuf = null;
  private ArrayList<Long> inflight = new ArrayList<Long>();

  private final TimerTask watcherTask;

  private volatile boolean isClosed = false;

  /**
   * Instantiates a new Off heap storage area based space.
   *
   * @param location the location
   * @param pgSz the pg sz
   */
  public OffHeapStorageAreaBasedSpace(PageSourceLocation location, int pgSz) {
    this.storeLocation = location;
    offheap = new OffHeapStorageArea(PointerSize.LONG, null, location.getPageSource(), pgSz, false, false);
    this.pgSize = pgSz;
    this.watcherTask = watch(20);
  }

  public OffHeapStorageAreaBasedSpace(PageSource source, long size, int maxChunk, int pgSz) {
    this.storeLocation = new PageSourceLocation(source, size, maxChunk);
    offheap = new OffHeapStorageArea(PointerSize.LONG, null, source, 4 * 1024, 8 * 1024 * 1024, false, false);
    this.pgSize = pgSz;
    this.watcherTask = watch(20);
  }

  private TimerTask watch(int i) {
    return TimedWatcher.watch(i * 1000, new Runnable() {
      @Override
      public void run() {
        final OffHeapStorageArea storageArea = OffHeapStorageAreaBasedSpace.this.offheap;
        if (storageArea == null) {
          return;
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println("OffheapSA; Allocated: " + MiscUtils.bytesAsNiceString(
          storageArea.getAllocatedMemory()) + " Occupied: " + MiscUtils.bytesAsNiceString(storageArea.getOccupiedMemory()));
        pw.flush();
        pw.close();
        System.out.println(sw.toString());
      }
    });
  }

  @Override
  public long append(ByteBuffer... srcs) throws IOException {
    this.testClosed();
    int total = 0;
    for (ByteBuffer b : srcs) {
      total = total + b.remaining();
    }
    long addr = offheap.allocate(total);
    if (addr < 0l) {
      throw new StoreOutOfSpaceException();
    }
    offheap.writeBuffers(addr, srcs);
    highWaterMark = Math.max(highWaterMark, addr + total);
    inflight.add(addr);
    return addr;
  }

  @Override
  public void discard() throws IOException {
    this.testClosed();
    for (long l : inflight) {
      offheap.free(l);
    }
    inflight = new ArrayList<Long>();
  }

  @Override
  public void commit(boolean durable, ByteBuffer cBuf) {
    this.testClosed();
    commitBuf = NIOBufferUtils.dup(cBuf.slice());
    inflight = new ArrayList<Long>();
  }

  /**
   * Gets commit data.
   *
   * @param buf the buf
   * @throws IOException the iO exception
   */
  @Override
  public void getCommitData(ByteBuffer buf) throws IOException {
    this.testClosed();
    if (commitBuf == null) {
      throw new IOException();
    }
    commitBuf.clear();
    NIOBufferUtils.copy(commitBuf, buf);
    buf.limit(buf.position() + commitBuf.remaining());
  }

  @Override
  public void storeFree(long addr, boolean immediate) {
    this.testClosed();
    if (addr >= 0) {
      offheap.free(addr);
    }
  }

  @Override
  public boolean supportsDurability() {
    return false;
  }

  @Override
  public void read(long addr, ByteBuffer dest) throws IOException {
    this.testClosed();
    if (addr + dest.remaining() > highWaterMark) {
      dest.limit((int) (highWaterMark - addr));
    }
    int many = dest.remaining();
    try {
      ByteBuffer buf = offheap.readBuffer(addr, many);
      dest.put(buf);
      dest.flip();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public ByteBuffer readOnly(long offset, int many) throws IOException {
    this.testClosed();
    ByteBuffer buf = offheap.readBuffer(offset, many);
    return buf.asReadOnlyBuffer();
  }

  @Override
  public void close() {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;

    this.watcherTask.cancel();

    this.offheap.destroy();
    this.offheap = null;
  }

  @Override
  public boolean supportsReadOnly() {
    return false;
  }

  /**
   * Gets page size.
   *
   * @return the page size
   */
  @Override
  public int getPageSize() {
    return 4096;
  }

  @Override
  public boolean isNew() {
    return true;
  }

  /**
   * Gets stats.
   *
   * @return the stats
   */
  @Override
  public SimpleStoreStats getStats() {

    return new SimpleStoreStats(SimpleStoreStats.StatsType.MEMORY) {
      @Override
      public long getUserBytes() {
        return offheap.getOccupiedMemory();
      }

      @Override
      public long getAllocatedBytes() {
        return offheap.getAllocatedMemory();
      }
    };
  }

  /**
   * Gets store location.
   *
   * @return the store location
   */
  @Override
  public StoreLocation getStoreLocation() {
    return storeLocation;
  }

  private void testClosed() throws IllegalStateException {
    if (this.isClosed) {
      throw new IllegalStateException("Attempt to use disposed OffHeapStorageAreaBasedSpace");
    }
  }
}
