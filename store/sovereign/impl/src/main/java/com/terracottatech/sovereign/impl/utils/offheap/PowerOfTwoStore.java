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
package com.terracottatech.sovereign.impl.utils.offheap;

import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.committers.MemoryCommitter;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.location.StoreLocation;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author cschanck
 **/
public class PowerOfTwoStore implements SimpleStore {
  private final PageSourceLocation psl;
  private final BlockStorageArea bs;
  private final MemoryCommitter committer;
  private final SimpleStoreStats stats;
  private final ArrayList<Long> pendingFrees;
  private boolean isClosed = false;

  public PowerOfTwoStore(PageSourceLocation psl, int blockSize, int minPageSize, int maxPageSize) {
    this.psl = psl;
    this.bs = new BlockStorageArea(psl.getPageSource(), blockSize, minPageSize, maxPageSize);
    this.committer = new MemoryCommitter();
    this.pendingFrees = new ArrayList<Long>();
    this.stats = new SimpleStoreStats(SimpleStoreStats.StatsType.MEMORY) {
      @Override
      public long getAllocatedBytes() {
        return (long) bs.getTotalBlocks() * bs.getBlockSize();
      }

      @Override
      public long slackBytes() {
        return getAllocatedBytes() - getUserBytes();
      }

      @Override
      public long getUserBytes() {
        return (long) (bs.getTotalBlocks() - bs.getFreeBlocks()) * bs.getBlockSize();
      }
    };
  }

  @Override
  public SimpleStoreStats getStats() {
    return stats;
  }

  @Override
  public StoreLocation getStoreLocation() {
    return psl;
  }

  @Override
  public void read(long offset, ByteBuffer buf) throws IOException {
    NIOBufferUtils.copy(bs.blockAt(offset), 0, buf, buf.position(), Math.min(buf.remaining(), bs.getBlockSize()));
  }

  @Override
  public ByteBuffer readOnly(long addr, int many) throws IOException {
    ByteBuffer b = bs.blockAt(addr);
    b.limit(many);
    return b.asReadOnlyBuffer();
  }

  @Override
  public void getCommitData(ByteBuffer buf) throws IOException {
    this.testClosed();
    committer.getCommitData(buf);
  }

  @Override
  public void commit(boolean durable, ByteBuffer buf) throws IOException {
    this.testClosed();
    committer.commit(buf);
    for (long l : pendingFrees) {
      storeFree(l, true);
    }
    pendingFrees.clear();
  }

  @Override
  public long append(ByteBuffer... bufs) throws IOException {
    testClosed();
    if (bufs.length != 1) {
      throw new UnsupportedOperationException("Multi buf append not supported!");
    }
    return append0(bufs[0]);
  }

  private long append0(ByteBuffer buf) {
    if (buf.remaining() > bs.getBlockSize()) {
      throw new UnsupportedOperationException("Allocation size larger than block size: " + buf.remaining());
    }
    long addr = bs.allocateBlock();
    NIOBufferUtils.copy(buf, buf.position(), bs.blockAt(addr), 0, buf.remaining());
    return addr;
  }

  @Override
  public void discard() throws IOException {
    this.testClosed();
    pendingFrees.clear();
  }

  @Override
  public void close() {
    if (!isClosed) {
      isClosed = true;
      bs.dispose();
    }
  }

  @Override
  public void storeFree(long addr, boolean immediate) throws IOException {
    if (immediate) {
      bs.freeBlock(addr);
    } else {
      pendingFrees.add(addr);
    }
  }

  @Override
  public boolean supportsDurability() {
    return false;
  }

  @Override
  public boolean supportsReadOnly() {
    return true;
  }

  @Override
  public int getPageSize() {
    return bs.getBlockSize();
  }

  @Override
  public boolean isNew() {
    return true;
  }

  private void testClosed() throws IllegalStateException {
    if (this.isClosed) {
      throw new IllegalStateException("Attempt to use disposed DiskFileSpace");
    }
  }
}
