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

import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.committers.CachedDurableCommitter;
import com.terracottatech.sovereign.btrees.stores.committers.DurableFileCommitter;
import com.terracottatech.sovereign.btrees.stores.location.DirectoryStoreLocation;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.location.StoreLocation;
import com.terracottatech.sovereign.common.utils.TimedWatcher;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Disk file space. Either mapped or unmapped. Core disk based storage.
 *
 * @author cschanck
 */
public class DiskFileSpace implements SimpleStore {
  private static final int MAXFILESIZE = 1024 * 1024 * 1024;
  private static final int MINFILESIZE = 64 * 1024;
  private final DirectoryStoreLocation dsl;
  private final int blockSize;
  private final PageSourceLocation bsl;
  private final boolean wasNew;
  private final BlockBufferFactory blockFactory;
  private final int pageSize;
  private final int maxFileSize;
  private int lastBlock = -1;
  private int huntDirection = 1;
  private final SimpleStoreStats stats;
  private final CachedDurableCommitter committer;
  private CopyOnWriteArrayList<SingleDiskFile> files = new CopyOnWriteArrayList<SingleDiskFile>();
  private int nextFileSize;
  private ArrayList<Long> inflight = new ArrayList<Long>();

  private volatile boolean isClosed = false;

  /**
   * Instantiates a new Disk file space.
   *
   * @param dsl             the dsl
   * @param bsl             the bsl
   * @param blockFactory    the block factory
   * @param seedFileSize    the seed file size
   * @param maxSeedFileSize the max seed file size
   * @param pageSize        the page size
   * @throws IOException the iO exception
   */
  public DiskFileSpace(DirectoryStoreLocation dsl, PageSourceLocation bsl, BlockBufferFactory blockFactory,
                       int seedFileSize, int maxSeedFileSize, int pageSize) throws IOException {
    this.dsl = dsl;
    this.bsl = bsl;
    this.pageSize = pageSize;
    this.blockSize = blockFactory.getBlockSize();
    this.maxFileSize = Math.max(MINFILESIZE, Math.min(maxSeedFileSize, MAXFILESIZE));
    this.nextFileSize = Math.min(Math.max(seedFileSize, MINFILESIZE), maxFileSize);

    stats = SimpleStoreStats.diskStats();
    this.blockFactory = blockFactory;
    if (dsl.exists()) {
      dsl.reopen();

      File[] allFiles = dsl.getVersioner().getAllVersionedFiles();
      for (File f : allFiles) {
        files.add(new SingleDiskFile(f, bsl, blockFactory, (int) f.length(), pageSize, stats));
      }
      wasNew = false;
    } else {
      wasNew = true;
    }
    dsl.ensure();
    this.committer = new CachedDurableCommitter(
      new DurableFileCommitter(new File(dsl.getVersioner().getDir(), "committer.commit"), 1024));

  }

  List<SingleDiskFile> getSingleFileSpaces() {
    return files;
  }

  protected int nextFileSize() {
    int ret = nextFileSize;
    if (nextFileSize < maxFileSize) {
      nextFileSize = Math.min(Math.abs(nextFileSize * 2), maxFileSize);
    }
    return ret;
  }

  private TimerTask watch(int i) {
    return TimedWatcher.watch(i * 1000, new Runnable() {
      @Override
      public void run() {
        dump();
      }
    });
  }

  private void dump() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    int i = files.size();
    pw.println("Disk: " + pageSize);
    long totalFootprint = 0;
    for (int j = 0; j < i; j++) {
      SingleDiskFile q = files.get(j);
      pw.println("\t" + j + ". " + q.toString());
      totalFootprint = totalFootprint + (q.footprint() / (1024 * 1024));
    }
    pw.println("\t" + stats);
    pw.println("Factory: " + blockFactory);
    pw.flush();
    pw.close();
    System.out.println(sw.toString());
  }

  @Override
  public boolean supportsDurability() {
    return true;
  }

  @Override
  public boolean supportsReadOnly() {
    return false;
  }

  @Override
  public StoreLocation getStoreLocation() {
    return dsl;
  }

  @Override
  public void read(long addr, ByteBuffer buf) throws IOException {
    this.testClosed();
    int index = (int) (addr >>> 32);
    int offset = (int) (addr & 0xffffffff);
    files.get(index).read(offset, buf);
  }

  @Override
  public ByteBuffer readOnly(long addr, int many) throws IOException {
    this.testClosed();
    ByteBuffer b1 = ByteBuffer.allocate(many);
    read(addr, b1);
    return b1;
  }

  @Override
  public long append(ByteBuffer... srcs) throws IOException {
    this.testClosed();
    int room = 0;
    for (ByteBuffer b : srcs) {
      room = room + b.remaining();
    }
    for (int i = lastBlock; i >= 0 && i < files.size(); i = i + huntDirection) {
      SingleDiskFile q = files.get(i);
      if (q != null && q.hasRoomFor(room)) {
        return allocFrom(i, srcs, room);
      }
    }

    huntDirection = huntDirection * -1;
    for (int i = lastBlock + huntDirection; i >= 0 && i < files.size(); i = i + huntDirection) {
      SingleDiskFile q = files.get(i);
      if (q != null && q.hasRoomFor(room)) {
        return allocFrom(i, srcs, room);
      }
    }

    int next = files.size();
    huntDirection = -1;
    files.add(makeSingleFile());
    long ret = allocFrom(next, srcs, room);
    return ret;
  }

  private long allocFrom(int block, ByteBuffer[] srcs, int total) throws IOException {
    SingleDiskFile single = files.get(block);
    int offset = single.allocateAndWrite(srcs);
    lastBlock = block;
    long ret = ((long) block) << 32 | ((long) offset);
    inflight.add(ret);
    return ret;
  }

  private SingleDiskFile makeSingleFile() throws IOException {
    return new SingleDiskFile(dsl.getVersioner().getNextVersionedFile(), bsl, blockFactory, nextFileSize(), pageSize,
      stats);
  }

  @Override
  public void discard() throws IOException {
    this.testClosed();
    for (long l : inflight) {
      storeFree(l, true);
    }
    inflight = new ArrayList<Long>();
  }

  @Override
  public void getCommitData(ByteBuffer buf) throws IOException {
    this.testClosed();
    committer.getCommitData(buf);
  }

  @Override
  public void commit(boolean durable, ByteBuffer buf) throws IOException {
    this.testClosed();
    for (int i = 0; i < files.size(); i++) {
      SingleDiskFile q = files.get(i);
      if (q != null) {
        q.flush(durable);
      }
    }
    committer.commit(durable, buf);
    inflight = new ArrayList<Long>();
    huntDirection = 1;
    lastBlock = 0;
  }

  @Override
  public void close() throws IOException {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;

    blockFactory.close();
    for (SingleDiskFile sf : files) {
      sf.flush(true);
      sf.close();
    }
    committer.close();
    files.clear();
  }

  @Override
  public void storeFree(long addr, boolean immediate) throws IOException {
    this.testClosed();
    int index = (int) (addr >>> 32);
    int offset = (int) (addr & 0xffffffff);
    files.get(index).free(offset, immediate);
  }

  @Override
  public SimpleStoreStats getStats() {
    return stats;
  }

  @Override
  public int getPageSize() {
    return pageSize;
  }

  @Override
  public boolean isNew() {
    return wasNew;
  }

  private void testClosed() throws IllegalStateException {
    if (this.isClosed) {
      throw new IllegalStateException("Attempt to use disposed DiskFileSpace");
    }
  }
}
