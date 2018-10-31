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
package com.terracottatech.sovereign.impl.utils.batchsort;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

public class OffheapSortIndex implements SortIndex {
  private final int chunkCount;
  private final long chunkMask;
  private final int chunkShift;
  private ArrayList<LongBuffer> chunkList = new ArrayList<>();
  private long counter = 0;
  private int currentNext = 0;
  private LongBuffer current = null;

  public OffheapSortIndex(int chunkCount) {
    if (Integer.bitCount(chunkCount) != 1 || chunkCount < 0 || chunkCount > 512 * 1024 / 8) {
      throw new IllegalArgumentException();
    }

    this.chunkCount = chunkCount;
    this.chunkMask = this.chunkCount - 1;
    this.chunkShift = Long.bitCount(chunkMask);
    currentNext = chunkCount + 1;
  }

  private LongBuffer chunkFor(long index) {
    long i = index >>> chunkShift;
    return chunkList.get((int) i);
  }

  private int offsetOf(long index) {
    return (int) (index & chunkMask);
  }

  @Override
  public long addressOf(long index) {
    return chunkFor(index).get(offsetOf(index));
  }

  @Override
  public void add(long address) {
    ensureRoom();
    int idx = currentNext++;
    current.put(idx, address);
    counter++;
  }

  private void ensureRoom() {
    if (currentNext < chunkCount) {
      return;
    }
    current = ByteBuffer.allocateDirect(chunkCount * Long.BYTES).asLongBuffer();
    chunkList.add(current);
    currentNext = 0;
  }

  public void set(long index, long address) {
    chunkFor(index).put(offsetOf(index), address);
  }

  @Override
  public void swap(long index1, long index2) {
    long tmp = addressOf(index1);
    set(index1, addressOf(index2));
    set(index2, tmp);
  }

  @Override
  public void clear() {
    chunkList.clear();
    counter = 0;
    currentNext = 0;
    current = null;
  }

  @Override
  public long size() {
    return counter;
  }

  @Override
  public LongStream inOrderStream() {
    Spliterator.OfLong split = Spliterators.spliteratorUnknownSize(new PrimitiveIterator.OfLong() {
      int ch=0;
      int pos=0;
      long cnt=0;
      @Override
      public long nextLong() {
        if(hasNext()) {
          long ret = chunkList.get(ch).get(pos++);
          cnt++;
          if(!hasNext()) {
            ch++;
            pos=0;
          }
          return ret;
        }
        throw new NoSuchElementException();
      }

      @Override
      public boolean hasNext() {
        return chunkList.size()>ch && chunkList.get(ch).capacity()>pos && cnt<size();
      }
    }, Spliterator.ORDERED);
    return StreamSupport.longStream(split, false);
  }

  @Override
  public void dispose() {
    clear();
  }

  @Override
  public SortIndex duplicate() {
    OffheapSortIndex tmp = new OffheapSortIndex(chunkCount);
    for (LongBuffer lb : chunkList) {
      LongBuffer t = ByteBuffer.allocateDirect(Long.BYTES * lb.capacity()).asLongBuffer();
      t.put(lb.slice());
      t.clear();
      tmp.chunkList.add(t);
    }
    tmp.currentNext = currentNext;
    tmp.counter = counter;
    if (tmp.chunkList.isEmpty()) {
      tmp.current = null;
    } else {
      tmp.current = tmp.chunkList.get(tmp.chunkList.size() - 1);
    }
    return tmp;
  }
}
