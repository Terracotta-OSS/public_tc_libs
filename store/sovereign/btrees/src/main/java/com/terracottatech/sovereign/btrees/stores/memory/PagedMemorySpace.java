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
import com.terracottatech.sovereign.btrees.stores.committers.MemoryCommitter;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.location.StoreLocation;
import com.terracottatech.sovereign.common.utils.TimedWatcher;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Primary in memory store. Backed by a set of SingleBlocks, uses a specified
 * buffer source.
 *
 * @author cschanck
 */
public class PagedMemorySpace implements SimpleStore {

  static final int OVERHEAD = 4;
  private static final int MAX_BLOCK_SIZE = 1024 * 1024 * 1024;
  private static final int MIN_BLOCK_SIZE = 1 * 1024;
  private final SimpleStoreStats stats;
  private final PageSourceLocation location;
  private final int maxBlockSize;
  private CopyOnWriteArrayList<SinglePagedMemorySpace> queues = new CopyOnWriteArrayList<SinglePagedMemorySpace>();
  private volatile int lastSingleQueue = -1;
  private final int pageSize;
  private MemoryCommitter committer = new MemoryCommitter();
  private int nextBlockSize;
  private ArrayList<Long> inflight = new ArrayList<Long>();

  private volatile boolean isClosed = false;

  /**
   * Instantiates a new Buffer source paged memory space.
   *
   * @param location the location
   * @param pageSize the page size
   * @param initialBlockSize the initial block size
   */
  public PagedMemorySpace(PageSourceLocation location, int pageSize, int initialBlockSize, int maxBlockSize) {
    this.pageSize = pageSize + OVERHEAD;
    this.location = new PageSourceLocation(location);

    this.nextBlockSize = Math.min(Math.max(initialBlockSize, MIN_BLOCK_SIZE), maxBlockSize);
    this.maxBlockSize = Math.max(nextBlockSize, Math.min(this.location.getMaxAllocationSize(), maxBlockSize));

    this.stats = new SimpleStoreStats(SimpleStoreStats.StatsType.MEMORY);
  }

  public PageSourceLocation getLocation() {
    return location;
  }

  private int nextSize() {
    int i = nextBlockSize;
    if (nextBlockSize < MAX_BLOCK_SIZE) {
      nextBlockSize = Math.min(Math.abs(nextBlockSize * 2), maxBlockSize);
    }
    return i;
  }

  @Override
  public void commit(boolean durable, ByteBuffer cBuf) {
    this.testClosed();
    committer.commit(cBuf);
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
    committer.getCommitData(buf);
  }

  private TimerTask watch(int i) {
    return TimedWatcher.watch(i * 1000, new Runnable() {
      @Override
      public void run() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        int i = queues.size();
        pw.println("PagedMemorySpace: " + pageSize);
        pw.println("\t" + getStats());
        long totalFootprint = 0;
        for (int j = 0; j < i; j++) {
          SinglePagedMemorySpace q = queues.get(j);
          pw.println(
            "\t" + j + ". " + q.allocated() + "/" + q.freed() + " foot: " + (q.footprint() / (1024 * 1024)) + "M");
          totalFootprint = totalFootprint + (q.footprint() / (1024 * 1024));
        }
        pw.println("\tTotal Footprint: " + totalFootprint + "M");
        pw.flush();
        pw.close();
        System.out.println(sw.toString());
      }
    });
  }

  public long append(ByteBuffer... srcs) throws StoreOutOfSpaceException {
    this.testClosed();
    int size = 0;
    for (ByteBuffer b : srcs) {
      size = size + b.remaining();
    }
    if (lastSingleQueue >= 0 && lastSingleQueue < queues.size()) {
      if (queues.get(lastSingleQueue).hasRoomFor(size)) {
        return allocFrom(lastSingleQueue, srcs, size);
      }
    }
    for (int i = 0; i < queues.size(); i++) {
      if (queues.get(i).hasRoomFor(size)) {
        return allocFrom(i, srcs, size);
      }
    }

    queues.add(new SinglePagedMemorySpace(location, nextSize(), pageSize, stats));
    long ret = allocFrom(queues.size() - 1, srcs, size);
    return ret;
  }

  @Override
  public void discard() throws IOException {
    this.testClosed();
    for (long l : inflight) {
      storeFree(l, true);
    }
    inflight = new ArrayList<Long>();
  }

  private long allocFrom(int index, ByteBuffer[] srcs, int size) {
    SinglePagedMemorySpace singleQueue = queues.get(index);
    int offset = singleQueue.append(srcs);
    lastSingleQueue = index;
    long ret = ((long) index) << 32 | ((long) offset);
    inflight.add(ret);
    return ret;
  }

  public void storeFree(long addr, boolean immediate) {
    this.testClosed();
    int index = (int) (addr >>> 32);
    int offset = (int) (addr & 0xffffffff);
    queues.get(index).free(offset);
  }

  @Override
  public boolean supportsDurability() {
    return false;
  }

  public void read(long addr, ByteBuffer dest) throws IOException {
    this.testClosed();
    int index = (int) (addr >>> 32);
    int offset = (int) (addr & 0xffffffff);
    queues.get(index).read(offset, dest);
  }

  @Override
  public ByteBuffer readOnly(long addr, int many) throws IOException {
    this.testClosed();
    if (many > (pageSize - OVERHEAD)) {
      ByteBuffer b = ByteBuffer.allocate(many);
      read(addr, b);
      b.clear();
      return b.asReadOnlyBuffer();
    }
    int index = (int) (addr >>> 32);
    int offset = (int) (addr & 0xffffffff);
    return queues.get(index).readOnly(offset, many);
  }

  @Override
  public void close() {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;

    location.freeAll();
    queues.clear();
  }

  @Override
  public boolean supportsReadOnly() {
    return true;
  }

  /**
   * Gets page size.
   *
   * @return the page size
   */
  @Override
  public int getPageSize() {
    return pageSize - OVERHEAD;
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
    return stats;
  }

  /**
   * Gets store location.
   *
   * @return the store location
   */
  @Override
  public StoreLocation getStoreLocation() {
    return location;
  }

  private void testClosed() throws IllegalStateException {
    if (this.isClosed) {
      throw new IllegalStateException("Attempt to use disposed DiskFileSpace");
    }
  }
}
