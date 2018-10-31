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

import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import org.terracotta.offheapstore.paging.PageSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author cschanck
 **/
public class BlockStorageArea {

  private final PageSource source;
  private final int blockNumberMask;
  private final int blockShift;
  private final int minSize;
  private int nextSize;
  private final int maxSize;
  private final int blockSize;
  private int allocHand;
  private ArrayList<MicroBlock> blocks = new ArrayList<>();
  private ArrayList<MicroBlock> blocksByFree = new ArrayList<>();
  public int freeBlocks = 0;
  public int totalBlocks = 0;

  public BlockStorageArea(PageSource source, int blockSize, int minPageSize, int maxPageSize) {
    this.source = source;
    if (Integer.bitCount(minPageSize) != 1) {
      throw new IllegalArgumentException("min size must be power of 2: " + minPageSize);
    }
    if (Integer.bitCount(maxPageSize) != 1) {
      throw new IllegalArgumentException("max size must be power of 2: " + maxPageSize);
    }
    if (minPageSize > maxPageSize) {
      throw new IllegalArgumentException("max size must be >= min: " + maxPageSize + " / " + minPageSize);
    }
    if (minPageSize < blockSize) {
      throw new IllegalArgumentException("block size must be <= min page size: " + blockSize + " / " + minPageSize);
    }
    this.blockSize = blockSize;
    int bper = maxPageSize / blockSize;
    int val = 1;
    while (val < bper) {
      val = val << 1;
    }

    this.blockNumberMask = val - 1;
    this.blockShift = Integer.numberOfTrailingZeros(val);

    this.nextSize = minPageSize;
    this.minSize = minPageSize;
    this.maxSize = maxPageSize;
    this.allocHand = 0;
  }

  public long allocateBlock() {
    for (int i = allocHand; i < blocksByFree.size(); i++) {
      MicroBlock mb = blocksByFree.get(i);
      int p = mb.allocateBlock();
      if (p >= 0) {
        allocHand = i;
        freeBlocks--;
        return makeAddress(mb.getBlockId(), p);
      }
    }
    sortbyUsage();
    for (int i = 0; i < 1 && i < blocksByFree.size(); i++) {
      MicroBlock mb = blocksByFree.get(i);
      int p = mb.allocateBlock();
      if (p >= 0) {
        allocHand = i;
        freeBlocks--;
        return makeAddress(mb.getBlockId(), p);
      }
    }
    // no room at the inn
    int index = blocks.size();
    MicroBlock mb = makeNewBlock(index);
    totalBlocks = totalBlocks + mb.getNumBlocks();
    freeBlocks = freeBlocks + mb.getFreeCount();
    blocks.add(mb);
    blocksByFree.add(mb);
    // cheat, use the new one at the end, put off sorting for a while.
    allocHand = blocks.size() - 1;
    int p = mb.allocateBlock();
    if (p >= 0) {
      freeBlocks--;
      return makeAddress(index, p);
    }
    return -1l;
  }

  private MicroBlock makeNewBlock(int index) {
    // here we are going to try to allocate nextSize
    for (; ; ) {
      int trySize = nextSize;
      try {
        MicroBlock ret = new MicroBlock(source, blockSize, trySize, index);
        // success, double the next size if you can
        if (nextSize < maxSize) {
          nextSize = nextSize * 2;
        }
        return ret;
      } catch (SovereignExtinctionException see) {
        // hah, failure. if we are <= the minSize, boom, done. Fail.
        if (trySize <= minSize) {
          nextSize = minSize;
          throw see;
        }
        // halve our request size and move forward.
        nextSize = trySize / 2;
      }
    }
  }

  private static final Comparator<MicroBlock> byUsage = new Comparator<MicroBlock>() {
    @Override
    public int compare(MicroBlock o1, MicroBlock o2) {
      int ret = o2.getFreeCount() - o1.getFreeCount();
      return ret;
    }
  };

  private void sortbyUsage() {
    blocksByFree.sort(byUsage);
  }

  private long makeAddress(int index, int block) {
    long a = index;
    a = a << blockShift;
    a = a | block;
    return a;
  }

  public ByteBuffer blockAt(long addr) {
    int index = blockIndex(addr);
    int block = blockNum(addr);
    return blocks.get(index).blockAt(block);
  }

  private int blockNum(long addr) {
    return (int) (addr & blockNumberMask);
  }

  private int blockIndex(long addr) {
    return (int) (addr >> blockShift);
  }

  public void freeBlock(long addr) {
    int index = blockIndex(addr);
    int block = blockNum(addr);
    blocks.get(index).freeBlock(block);
    freeBlocks++;
  }

  public void clear() {
    for (MicroBlock mb : blocks) {
      mb.dispose();
    }
    blocks.clear();
    blocksByFree.clear();
    allocHand = 0;
    freeBlocks = 0;
    totalBlocks = 0;
  }

  public void dispose() {
    clear();
  }

  public int getBlockSize() {
    return blockSize;
  }

  public int getFreeBlocks() {
    return freeBlocks;
  }

  public int getTotalBlocks() {
    return totalBlocks;
  }
}
