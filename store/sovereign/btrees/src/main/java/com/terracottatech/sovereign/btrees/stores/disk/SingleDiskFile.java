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

import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.common.utils.MiscUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;

import static com.terracottatech.sovereign.common.utils.FileUtils.fileChannel;

/**
 * One single disk file, currently limited to 32 bits in length. A DiskFileSPace is made up of multiple
 * of these.
 *
 * @author cschanck
 */
public class SingleDiskFile {
  private static final int PAGE_OVERHEAD = 4;

  private final int totalSize;
  private final int blockSize;
  private final File file;
  private final int maxBlocks;
  private final int pageSize;
  private final int maxPages;
  private final int pagesPerBlock;
  private final BlockBuffer[] blocks;
  private final SimpleStoreStats stats;
  private final int offsetMask;
  private final int blockShift;
  private final FileChannel channelProvider;
  private final BlockBuffer[] blocksByUsage;
  private int freeCount = 0;
  private int freeHand = -1;
  private int stagedFreeCount = 0;
  private IntBuffer stagedFreeBuf;
  private int allocatedBytes = 0;
  private HashSet<Integer> allocated = null;

  /**
   * Instantiates a new Single disk file.
   *
   * @param f                  the f
   * @param bsl                the bsl
   * @param blockFactory       the block factory
   * @param requestedTotalSize the requested total size
   * @param pgSize             the pg size
   * @param stats              the stats
   * @throws IOException the iO exception
   */
  public SingleDiskFile(File f, PageSourceLocation bsl, BlockBufferFactory blockFactory, int requestedTotalSize,
                        int pgSize, SimpleStoreStats stats) throws IOException {
    this.blockSize = blockFactory.getBlockSize();
    this.pageSize = pgSize + PAGE_OVERHEAD;
    this.pagesPerBlock = this.blockSize / this.pageSize;
    if (pagesPerBlock > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Pages per block too large: " + pagesPerBlock + ".");
    }
    if (pagesPerBlock <= 0) {
      throw new IllegalArgumentException("Zero pages per block.");
    }
    this.totalSize = requestedTotalSize;
    this.maxBlocks = totalSize / blockSize;
    this.maxPages = maxBlocks * this.pagesPerBlock;
    this.offsetMask = calcOffsetMask(blockSize);
    this.blockShift = calcBlockShift(offsetMask);
    this.file = f;
    this.channelProvider = fileChannel(f);
    channelProvider.write(ByteBuffer.wrap(new byte[1]), totalSize - 1);

    blocks = new BlockBuffer[maxBlocks];
    blocksByUsage = new BlockBuffer[maxBlocks];
    for (int i = 0; i < this.maxBlocks; i++) {
      blocks[i] = blockFactory.make(i, channelProvider, i * this.blockSize);
      blocksByUsage[i] = blocks[i];
    }

    for (int b = 0; b < this.maxBlocks; b++) {
      blocks[b].initAllocations(pagesPerBlock);
      for (int p = 0; p < pagesPerBlock; p++) {
        enfreeDurably(blocks[b], p * pageSize);
      }
    }

    stagedFreeBuf = bsl.allocateBuffer(this.maxPages * 4).asIntBuffer();

    verifyStructure();
    this.stats = stats;
    stats.incrementAllocatedBytes(totalSize, stagedFreeBuf.capacity() * 4);
    System.out.println("Allocated file: " + f + " for " + MiscUtils.bytesAsNiceString(totalSize));
    sortBlocksByUsage();
  }

  private void sortBlocksByUsage() {
    Comparator<BlockBuffer> comp = new Comparator<BlockBuffer>() {
      @Override
      public int compare(BlockBuffer o1, BlockBuffer o2) {
        return o2.getFreePages() - o2.getFreePages();
      }
    };
    Arrays.sort(blocksByUsage, comp);
    freeHand = 0;
  }

  private void verifyStructure() throws IOException {
    File f = new File(file.getPath() + ".struct");
    Properties p = new Properties();
    if (f.exists()) {
      FileInputStream fis = new FileInputStream(f);
      try {
        p.load(fis);
        String clz = p.getProperty("class", "");
        if (!clz.equals(this.getClass().getName())) {
          throw new IOException("Mismatch in store class! Actual: " + this.getClass() + " expected: " + clz);
        }
        long tmp = Long.parseLong(p.getProperty("totalSize"));
        if (tmp != totalSize) {
          throw new IOException("Mismatch in total size: actual: " + tmp + " expected: " + totalSize);
        }
        tmp = Long.parseLong(p.getProperty("blockSize"));
        if (tmp != blockSize) {
          throw new IOException("Mismatch in block size: actual: " + tmp + " expected: " + blockSize);
        }
        tmp = Long.parseLong(p.getProperty("pageSize"));
        if (tmp != pageSize) {
          throw new IOException("Mismatch in page size: actual: " + tmp + " expected: " + pageSize);
        }
      } finally {
        fis.close();
      }
    } else {
      FileOutputStream fos = new FileOutputStream(f);
      try {
        p.setProperty("class", this.getClass().getName());
        p.setProperty("totalSize", Integer.toString(totalSize));
        p.setProperty("blockSize", Integer.toString(blockSize));
        p.setProperty("pageSize", Integer.toString(pageSize));
        p.store(fos, "no comment");
      } finally {
        fos.close();
      }
    }
  }

  BlockBuffer[] getBlocks() {
    return blocks;
  }

  public int getMaxBlocks() {
    return maxBlocks;
  }

  private int calcBlockShift(int mask) {
    int shiftAmount = 32 - Integer.numberOfLeadingZeros(mask);
    return shiftAmount;
  }

  private int calcOffsetMask(int max) {
    int mask = Integer.highestOneBit(max) - 1;
    return mask;
  }

  private int makeFileAddress(int b, int p) {
    return (b << blockShift) ^ p;
  }

  private int extractBlockIndex(int addr) {
    return (addr >>> blockShift);
  }

  private int extractPageOffset(int addr) {
    return addr & offsetMask;
  }

  private void enfreeDurably(BlockBuffer buf, int offset) {
    buf.free(offset);
    freeCount++;
  }

  private void enfreeStaged(int offset) {
    stagedFreeBuf.put(stagedFreeCount++, offset);
  }

  private int fromFree() {
    for (int i = freeHand; i < blocksByUsage.length; i++) {
      if (blocksByUsage[i].getFreePages() > 0) {
        freeCount--;
        freeHand = i;
        return makeFileAddress(blocksByUsage[i].getId(), blocksByUsage[i].alloc());
      }
    }
    freeHand = 0;
    for (int i = freeHand; i < blocksByUsage.length; i++) {
      if (blocksByUsage[i].getFreePages() > 0) {
        freeCount--;
        freeHand = i;
        return makeFileAddress(blocksByUsage[i].getId(), blocksByUsage[i].alloc());
      }
    }
    throw new OutOfMemoryError();
  }

  public boolean hasRoomFor(int size) {
    int numPages = size / (pageSize - PAGE_OVERHEAD);
    return (freeCount > numPages);
  }

  public void free(int offset, boolean immediate) throws IOException {
    do {
      int bIndex = extractBlockIndex(offset);
      int pIndex = extractPageOffset(offset);
      BlockBuffer buf = blocks[bIndex];
      int next = buf.readInt(pIndex - PAGE_OVERHEAD);
      if (immediate) {
        enfreeDurably(buf, pIndex - PAGE_OVERHEAD);
      } else {
        enfreeStaged(offset - PAGE_OVERHEAD);
      }
      offset = next;
      stats.incrementUserBytes(0 - (pageSize - PAGE_OVERHEAD));
      allocatedBytes = allocatedBytes - (pageSize - PAGE_OVERHEAD);
    } while (offset >= 0);

  }

  public int allocateAndWrite(ByteBuffer... bufs) throws IOException {
    int currentBuffer = 0;
    int ret = fromFree();
    currentBuffer = fillOnePage(bufs, currentBuffer, ret);
    int lastOffset = ret;
    while (currentBuffer < bufs.length) {
      int next = fromFree();
      currentBuffer = fillOnePage(bufs, currentBuffer, next);
      int pbIndex = extractBlockIndex(lastOffset);
      int pOffset = extractPageOffset(lastOffset);
      BlockBuffer lastBuf = blocks[pbIndex];
      lastBuf.writeInt(pOffset, next + PAGE_OVERHEAD);
      lastOffset = next;
    }

    return ret + PAGE_OVERHEAD;
  }

  private int fillOnePage(ByteBuffer[] bufs, int bIndex, int addr) throws IOException {
    int room = pageSize - PAGE_OVERHEAD;
    int pbIndex = extractBlockIndex(addr);
    int pOffset = extractPageOffset(addr);
    BlockBuffer b = blocks[pbIndex];
    b.writeInt(pOffset, -1);
    pOffset = pOffset + PAGE_OVERHEAD;
    while (room > 0) {
      while (bIndex < bufs.length && !bufs[bIndex].hasRemaining()) {
        bIndex++;
      }
      if (bIndex < bufs.length) {
        int many = processOneBuf(bufs[bIndex], b, pOffset, room);
        room = room - many;
        pOffset = pOffset + many;
      } else {
        break;
      }
    }
    stats.incrementUserBytes(pageSize - PAGE_OVERHEAD);
    allocatedBytes = allocatedBytes + (pageSize - PAGE_OVERHEAD);
    return bIndex;
  }

  private int processOneBuf(ByteBuffer srcBuf, BlockBuffer destBuf, int offset, int room) throws IOException {
    int toMove = Math.min(room, srcBuf.remaining());
    int saveLimit = srcBuf.limit();
    srcBuf.limit(srcBuf.position() + toMove);
    destBuf.write(offset, srcBuf);
    srcBuf.limit(saveLimit);
    return toMove;
  }

  public int read(int offset, ByteBuffer dest) throws IOException {
    int many = 0;
    while (dest.hasRemaining() && offset >= 0) {
      int m = Math.min(dest.remaining(), pageSize - PAGE_OVERHEAD);
      int pbIndex = extractBlockIndex(offset);
      int pOffset = extractPageOffset(offset);
      BlockBuffer buf = blocks[pbIndex];
      int saveLimit = dest.limit();
      dest.limit(dest.position() + m);
      buf.read(pOffset, dest);
      dest.limit(saveLimit);
      many = many + m;
      offset = buf.readInt(pOffset - PAGE_OVERHEAD);
    }
    return many;
  }

  public int allocated() {
    return maxPages - (freeCount + stagedFreeCount);
  }

  public int durableFree() {
    return freeCount;
  }

  public int stagedFree() {
    return stagedFreeCount;
  }

  public long footprint() {
    return (long) totalSize + (long) maxPages * 8L;
  }

  public boolean flush(boolean durable) throws IOException {
    if (durable) {
      boolean flushed = false;
      for (BlockBuffer bb : blocks) {
        flushed = flushed || bb.flush();
      }
      moveStagedFreesToDurableFrees();
      channelProvider.force(false);
      return true;
    }
    return false;
  }

  private boolean moveStagedFreesToDurableFrees() {
    while (stagedFreeCount > 0) {
      int addr = stagedFreeBuf.get(--stagedFreeCount);
      BlockBuffer buf = blocks[extractBlockIndex(addr)];
      int offset = extractPageOffset(addr);
      enfreeDurably(buf, offset);
    }
    sortBlocksByUsage();
    return (freeCount == maxPages);
  }

  public ByteBuffer readOnly(int addr, int many) throws IOException {
    ByteBuffer b = ByteBuffer.allocate(many);
    read(addr, b);
    b.clear();
    return b;
  }

  public void close() throws IOException {
    for (int i = 0; i < blocks.length; i++) {
      blocks[i].close();
      blocks[i] = null;
    }
    channelProvider.close();
  }

  public File getFile() {
    return file;
  }

  public String toString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.print(
      "A:" + allocated() + " S:" + stagedFree() + " F:" + durableFree() + " foot: " + MiscUtils.bytesAsNiceString(
        footprint()));
    pw.flush();
    return sw.toString();
  }

  public void seedAllocation(int offset) throws IOException {
    if (allocated == null) {
      allocated = new HashSet<Integer>();
    }
    int tmp = extractPageOffset(offset) - PAGE_OVERHEAD;
    if (tmp % pageSize != 0) {
      throw new IllegalArgumentException("Region address: " + offset + " is not on a page boundary!");
    }

    while (offset >= 0) {
      allocated.add(offset - PAGE_OVERHEAD);
      int pbIndex = extractBlockIndex(offset);
      int pOffset = extractPageOffset(offset);
      BlockBuffer buf = blocks[pbIndex];
      offset = buf.readInt(pOffset - PAGE_OVERHEAD);
    }
  }

  public void finishSeedingAllocations() {
    if (allocated != null) {
      stats.incrementUserBytes(0L - allocatedBytes);
      allocatedBytes = 0;
      stagedFreeCount = 0;
      freeCount = 0;
      for (int b = this.maxBlocks - 1; b >= 0; b--) {
        BlockBuffer block = blocks[b];
        block.initAllocations(pagesPerBlock);
        for (int p = pagesPerBlock - 1; p >= 0; p--) {
          int addr = makeFileAddress(b, p * pageSize);
          if (!allocated.contains(addr)) {
            enfreeDurably(block, p * pageSize);
          }
        }
      }
      sortBlocksByUsage();
      allocatedBytes = maxBlocks * pagesPerBlock * (pageSize - PAGE_OVERHEAD) - freeCount * (pageSize - PAGE_OVERHEAD);
      stats.incrementUserBytes(allocatedBytes);
      allocated = null;
    }
  }

}
