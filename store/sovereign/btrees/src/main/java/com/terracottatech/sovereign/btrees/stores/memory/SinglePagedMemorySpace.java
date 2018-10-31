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

import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.StoreOutOfSpaceException;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 */
class SinglePagedMemorySpace {
  private final int pageSize;
  private final SimpleStoreStats stats;
  private final PageSourceLocation bsl;
  private final ByteBuffer buf;
  private final int singleBlockSize;
  private int freeCount = 0;
  private int freeHead = -1;
  private final int max;

  /**
   * Instantiates a new Single block.
   */
  public SinglePagedMemorySpace(PageSourceLocation bsl, int size, int pageSize, SimpleStoreStats stats) throws StoreOutOfSpaceException {
    this.bsl = bsl;
    this.singleBlockSize = size;
    this.pageSize = pageSize;
    this.stats = stats;
    this.max = singleBlockSize / pageSize;
    this.buf = allocateByteBuffer(singleBlockSize);
    this.buf.clear();
    for (int i = 0; i < max; i++) {
      enfree(i * pageSize);
    }
    stats.incrementAllocatedBytes(singleBlockSize);
  }

  private ByteBuffer allocateByteBuffer(int size) throws StoreOutOfSpaceException {
    ByteBuffer ret = bsl.allocateBuffer(size);
    if (ret == null) {
      throw new StoreOutOfSpaceException();
    }
    return ret;
  }

  private void enfree(int offset) {
    if (freeHead < 0) {
      buf.putInt(offset, -1);
    } else {
      buf.putInt(offset, freeHead);
    }
    freeHead = offset;
    freeCount++;
  }

  public int append(ByteBuffer... srcs) {
    int currentBuffer = 0;
    int ret = fromFree();
    currentBuffer = fillOnePage(srcs, currentBuffer, ret);
    int lastOffset = ret;
    while (currentBuffer < srcs.length && srcs[currentBuffer].hasRemaining()) {
      int next = fromFree();
      currentBuffer = fillOnePage(srcs, currentBuffer, next);
      buf.putInt(lastOffset, next + PagedMemorySpace.OVERHEAD);
      lastOffset = next;
    }

    return ret + PagedMemorySpace.OVERHEAD;
  }

  private int fillOnePage(ByteBuffer[] bufs, int bIndex, int addr) {
    int room = pageSize - PagedMemorySpace.OVERHEAD;
    buf.putInt(addr, -1);
    addr = addr + PagedMemorySpace.OVERHEAD;
    while (room > 0) {
      while (bIndex < bufs.length && !bufs[bIndex].hasRemaining()) {
        bIndex++;
      }
      if (bIndex < bufs.length) {
        ByteBuffer current = bufs[bIndex];
        int many = processOneBuf(current, addr, room);
        room = room - many;
        addr = addr + many;
      } else {
        break;
      }
    }
    stats.incrementUserBytes(pageSize - PagedMemorySpace.OVERHEAD);
    return bIndex;
  }

  private int processOneBuf(ByteBuffer srcBuf, int offset, int room) {
    int toMove = Math.min(room, srcBuf.remaining());
    int saveLimit = srcBuf.limit();
    srcBuf.limit(srcBuf.position() + toMove);
    NIOBufferUtils.copy(srcBuf, srcBuf.position(), buf, offset, toMove);
    srcBuf.limit(saveLimit);
    srcBuf.position(srcBuf.position() + toMove);
    return toMove;
  }

  private int fromFree() {
    int offset = freeHead;
    freeHead = buf.getInt(offset);
    freeCount--;
    return offset;
  }

  public boolean hasRoomFor(int size) {
    int numPages = size / (pageSize - PagedMemorySpace.OVERHEAD);
    return (freeCount > numPages);
  }

  public void free(int offset) {
    do {
      int actual = offset - PagedMemorySpace.OVERHEAD;
      int next = buf.getInt(actual);
      enfree(actual);
      offset = next;
      stats.incrementUserBytes(0 - (pageSize - PagedMemorySpace.OVERHEAD));
    } while (offset >= 0);
  }

  public int read(int offset, ByteBuffer dest) {
    int many = 0;
    while (dest.hasRemaining() && offset >= 0) {
      int m = Math.min(dest.remaining(), pageSize - PagedMemorySpace.OVERHEAD);
      NIOBufferUtils.copy(buf, offset, dest, dest.position(), m);
      many = many + m;
      offset = buf.getInt(offset - PagedMemorySpace.OVERHEAD);
      dest.position(dest.position() + m);
    }
    return many;
  }

  public int allocated() {
    return max - freed();
  }

  public int freed() {
    return freeCount + 1;
  }

  public long footprint() {
    return (long) buf.capacity();
  }

  public ByteBuffer readOnly(int offset, int max) {
    ByteBuffer b = buf.duplicate();
    b.position(offset);
    b.limit(offset + Math.min(b.remaining(), max));
    ByteBuffer slice = b.slice().asReadOnlyBuffer();
    return slice;
  }

  @Override
  public String toString() {
    return "SingleBlock{" +
      "size=" + MiscUtils.bytesAsNiceString(singleBlockSize) +
      ", freeCount=" + freeCount +
      ", freeHead=" + freeHead +
      ", max=" + max +
      '}';
  }

}
