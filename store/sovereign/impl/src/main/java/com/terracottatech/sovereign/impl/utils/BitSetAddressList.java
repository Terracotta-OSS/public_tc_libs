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
package com.terracottatech.sovereign.impl.utils;

import com.terracottatech.sovereign.impl.SovereignAllocationResource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * First, see {@link BitSetLongAddressListChunk} for the beginning witchery. This class adds
 * additional witchery of it's own. First, it does the simple thing and segments a mondo address
 * list into many chunks. This is not hard. However, it then uses the free entries to record a linked
 * list of free entries. If that makes sense. The unallocated entries form a linked list, so that
 * allocation is constant time. However, here we run into a problem. The underlying chunks demand
 * that the free entries not have their low bit set, so how do we record that slot 7 is free in the list?
 * Simple. Hah. Free slot values are recorded bit shifted by 1. Madness, yes. But if we have 63 bits
 * worth of slots, well, someone smarter than me can come fix it.
 * <p/>
 * Semi thread safe, in the sense that allocation/free is thread safe. Writes/reads to the same
 * slot are not thread safe, and must be cooordinated at a higher level.
 *
 * @author cschanck
 */
public class BitSetAddressList implements Iterable<Long> {

  static final byte RESERVE_OP = 0;
  static final byte SET_OP = 1;
  static final byte CLEAR_OP = 2;
  static final byte RESET_OP = 3;

  /**
   * Monitor:
   * byte op, long slot, long value
   * op = set,clear
   */

  private final long chunkMask;
  private final int bitsPerChunk;
  private final SovereignAllocationResource.BufferAllocator bufferSource;
  private final ArrayList<BitSetLongAddressListChunk> chunks = new ArrayList<>();
  private AtomicLong freeListHead = new AtomicLong(-1l);
  private AtomicLong allocatedSlots = new AtomicLong(0);
  private AtomicLong occupiedSlots = new AtomicLong(0);

  public BitSetAddressList(SovereignAllocationResource.BufferAllocator source, int bitsPerChunk) {
    this.bufferSource = source;
    this.chunkMask = (1 << bitsPerChunk) - 1l;
    this.bitsPerChunk = bitsPerChunk;
  }

  private int extractSlot(long slot) {
    return (int) (slot & chunkMask);
  }

  private int extractChunk(long slot) {
    long tmp = (slot >> bitsPerChunk);
    if ((tmp & 0xffffffff80000000L) != 0) {
      throw new IllegalArgumentException("Chunk index overflow: " + tmp);
    }
    return (int) tmp;
  }

  private void enfree(long slot) {
    int chunk = extractChunk(slot);
    int subslot = extractSlot(slot);
    chunks.get(chunk).clearToValue(subslot, freeListHead.get() << 1);
    freeListHead.set(slot);
  }

  public synchronized long reserve() {
    if (freeListHead.get() < 0) {
      allocateAndEnqueOneChunk();
    }
    long next = freeListHead.get();
    int chunk = extractChunk(next);
    int subslot = extractSlot(next);

    long nextFree = chunks.get(chunk).getClearValue(subslot) >> 1;

    freeListHead.set(nextFree);
    occupiedSlots.incrementAndGet();
    return next;
  }

  /**
   * This needs to be used only after a completely blank initialize.
   *
   * @param slot
   * @return slot
   */
  public synchronized long reserve(long slot) {
    int chunk = extractChunk(slot);
    while (chunk >= chunks.size()) {
      allocateOneChunk();
    }
    occupiedSlots.incrementAndGet();
    return slot;
  }

  /**
   * This method is only supposed to be used after
   * blank initialize has happened. It is only meant to
   * be used after a restart &amp; just before passive dataset
   * assumes the role of a active dataset
   */
  public synchronized void initializeFreeListByScan() {
    freeListHead.set(-1);
    for (int c = 0; c < chunks.size(); c++) {
      BitSetLongAddressListChunk chunk = chunks.get(c);
      for (int s = 0; s < chunk.getSlotCount(); s++) {
        if (!chunk.isInUse(s)) {
          long addr = makeAddress(c, s);
          enfree(addr);
        }
      }
    }
  }

  private void allocateOneChunk() {
    BitSetLongAddressListChunk newChunk = new BitSetLongAddressListChunk(bufferSource, bitsPerChunk);
    chunks.add(newChunk);
    allocatedSlots.addAndGet(newChunk.getSlotCount());
  }


  private void allocateAndEnqueOneChunk() {
    allocateOneChunk();
    int chunk = chunks.size() - 1;
    BitSetLongAddressListChunk newChunk = chunks.get(chunk);
    for (int subslot = newChunk.getSlotCount() - 1; subslot >= 0; subslot--) {
      enfree(makeAddress(chunk, subslot));
    }
  }

  private long makeAddress(int chunk, int slot) {
    return (((long) chunk) << bitsPerChunk) | slot;
  }

  public long get(long slot) {
    int chunk = extractChunk(slot);
    int subslot = extractSlot(slot);
    return chunks.get(chunk).getSetValue(subslot);
  }

  public void set(long slot, long value) {
    int chunk = extractChunk(slot);
    int subslot = extractSlot(slot);
    chunks.get(chunk).setToValue(subslot, value);
  }

  public long add(long value) {
    long nextSlot = reserve();
    set(nextSlot, value);
    return nextSlot;
  }

  public synchronized void clear(long slot) {
    int chunk = extractChunk(slot);
    int subslot = extractSlot(slot);
    chunks.get(chunk).clearToValue(subslot, 0);
    enfree(slot);
    occupiedSlots.decrementAndGet();
  }

  public synchronized void reset(long slot, long value) {
    int chunk = extractChunk(slot);
    int subslot = extractSlot(slot);
    chunks.get(chunk).resetToValue(subslot, value);
  }

  public long size() {
    return occupiedSlots.get();
  }

  public interface TrackingIterator extends Iterator<Long> {
  }

  @Override
  public TrackingIterator iterator() {
    return new TrackingIterator() {

      int curChunk = 0;
      int nextPos = 0;
      Long nextSlot = null;

      private void advanceNext() {
        if (nextSlot == null) {
          while (curChunk < chunks.size()) {
            while (nextPos < chunks.get(curChunk).getSlotCount()) {
              long ret = chunks.get(curChunk).getSetValue(nextPos);
              if (ret >= 0) {
                // cool
                nextSlot = makeAddress(curChunk, nextPos++);
                return;
              }
              nextPos++;
            }
            curChunk++;
            nextPos = 0;
          }
        }
      }

      @Override
      public boolean hasNext() {
        if (nextSlot == null) {
          advanceNext();
        }
        return nextSlot != null;
      }

      @Override
      public Long next() {
        if (nextSlot != null) {
          Long ret = nextSlot;
          nextSlot = null;
          advanceNext();
          return ret;
        }
        throw new NoSuchElementException();
      }
    };
  }

  public long getAllocatedSlots() {
    return allocatedSlots.get();
  }

  public synchronized void clear() {
    for (BitSetLongAddressListChunk c : chunks) {
      bufferSource.freeBuffer(c.sizeof());
    }
    chunks.clear();
    allocatedSlots.set(0);
    occupiedSlots.set(0);
    freeListHead.set(-1);
  }

  @Override
  public String toString() {
    return "BitSetAddressList{" +
      "bitsPerChunk=" + bitsPerChunk +
      ", chunks=" + chunks +
      '}';
  }
}
