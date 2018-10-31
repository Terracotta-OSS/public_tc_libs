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

import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public class MicroBlock {

  private final PageSource source;
  private final Page page;
  private final int blockId;
  private ByteBuffer buffer;
  private final int numBlocks;
  private final int blockSize;
  private int freeHead = -1;
  private int freeCount = 0;

  public MicroBlock(PageSource source, int blockSize, int totalSize, int blockId) {
    this.blockId = blockId;
    if (Integer.bitCount(totalSize) != 1 || totalSize < 0) {
      throw new IllegalArgumentException("total size is illegal: " + totalSize);
    }
    if (blockSize > totalSize || blockSize < 4) {
      throw new IllegalArgumentException("block size is illegal: " + blockSize);
    }
    this.source = source;
    this.page = source.allocate(totalSize, false, false, null);
    this.buffer = page.asByteBuffer();
    this.numBlocks = totalSize / blockSize;
    this.blockSize = blockSize;
    clear();
  }

  public int getBlockId() {
    return blockId;
  }

  public ByteBuffer blockAt(int block) {
    ByteBuffer tmp = buffer.duplicate();
    tmp.position(block * blockSize);
    tmp.limit(tmp.position() + blockSize);
    return tmp.slice();
  }

  public void freeBlock(int i) {
    blockAt(i).putInt(0, freeHead);
    freeHead = i;
    freeCount++;
  }

  public int allocateBlock() {
    if (freeHead < 0) {
      return -1;
    }
    int ret = freeHead;
    freeHead = blockAt(freeHead).getInt(0);
    freeCount--;
    return ret;
  }

  public int getNumBlocks() {
    return numBlocks;
  }

  public int getFreeCount() {
    return freeCount;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void clear() {
    // free them all;
    for (int i = 0; i < numBlocks; i++) {
      freeBlock(i);
    }
    freeCount = numBlocks;
  }

  public void dispose() {
    source.free(page);
    buffer = null;
    freeCount = 0;
    freeHead = -1;
  }
}
