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

import org.terracotta.offheapstore.buffersource.BufferSource;

import java.nio.LongBuffer;

/**
 * OK. This is a tiny bit complicated. In theory it manages a single fixed size LongBuffer. But,
 * it also manages whether a particular slot, or index, is allocated or not. This has to be fast.
 * The way we do this, is we steal the lowest bit. This means the long address's it is managing,
 * can only address even locations. This works out, because the memory allocator does at least
 * 16-bit alignment. A value that has the lowest bit set is allocated, one with a 0 lowest bit
 * is not allocated.
 *
 * @author cschanck
 */
public class BitSetLongAddressListChunk {

  private LongBuffer lb;

  public static int sizeof(int size) {
    int cap = 1 << size;
    return cap * 8;
  }

  public BitSetLongAddressListChunk(BufferSource bufferSource, int size) {
    if (size < 1 || size > 27) {
      throw new IllegalStateException();
    }
    int cap = 1 << size;
    lb = bufferSource.allocateBuffer(cap * 8).asLongBuffer();
  }

  public int sizeof() {
    return lb.capacity() * 8;
  }

  /**
   * Number of slots this chunk can hold
   *
   * @return
   */
  public int getSlotCount() {
    return lb.capacity();
  }

  /**
   * Is this slot holding a value.
   *
   * @param slot
   * @return
   */
  public boolean isInUse(int slot) {
    return getSetValue(slot) >= 0;
  }

  /**
   * Set the value, record the set state in the low bit.
   *
   * @param slot
   * @param value
   */
  public void setToValue(int slot, long value) {
    if ((value & 1l) != 0) {
      // oops.
      throw new IllegalArgumentException("Slot: " + slot + " Value: " + value);
    }
    value = value | 0x01l;
    lb.put(slot, value);
  }

  /**
   * Set it to a value, leaving the low bit clear.
   *
   * @param slot
   * @param value
   */
  public void clearToValue(int slot, long value) {
    if ((value & 1l) != 0) {
      // oops.
      throw new IllegalArgumentException("Slot: " + slot + " Value: " + value);
    }
    lb.put(slot, value);
  }

  /**
   * Get an expected to be set value.
   *
   * @param slot
   * @return
   */
  public long getSetValue(int slot) {
    long l = lb.get(slot);
    if ((l & 0x01l) != 0) {
      return l & 0xfffffffffffffffel;
    }
    return -1l;
  }

  /**
   * Get the clear value.
   *
   * @param slot
   * @return
   */
  public long getClearValue(int slot) {
    long l = lb.get(slot);
    if ((l & 0x01l) != 0) {
      // oops
      throw new IllegalStateException(l + "");
    }
    return l;
  }

  public void resetToValue(int slot, long value) {
    if ((value & 1l) != 0) {
      // oops.
      throw new IllegalArgumentException("Slot: " + slot + " Value: " + value);
    }
    if (!isInUse(slot)) {
      throw new IllegalArgumentException("Slot: " + slot + " Value: " + value);
    }
    value = value | 0x01l;
    lb.put(slot, value);

  }

}
