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
package com.terracottatech.sovereign.impl.memory.storageengines;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * @author cschanck
 *         <p/>
 *         Oh, and FU findbugs.
 **/
@SuppressFBWarnings({ "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE" })
public class SlotPrimitivePortability<T>
  implements PrimitivePortability<SlotPrimitivePortability.KeyAndSlot<T>>, Comparator<SlotPrimitivePortability.KeyAndSlot<T>> {

  public static final SlotPrimitivePortability<Boolean> BOOLEAN = new SlotPrimitivePortability<>(Boolean.class);
  public static final SlotPrimitivePortability<Byte> BYTE = new SlotPrimitivePortability<>(Byte.class);
  public static final SlotPrimitivePortability<Character> CHAR = new SlotPrimitivePortability<>(Character.class);
  public static final SlotPrimitivePortability<Integer> INT = new SlotPrimitivePortability<>(Integer.class);
  public static final SlotPrimitivePortability<Long> LONG = new SlotPrimitivePortability<>(Long.class);
  public static final SlotPrimitivePortability<Double> DOUBLE = new SlotPrimitivePortability<>(Double.class);
  public static final SlotPrimitivePortability<String> STRING = new SlotPrimitivePortability<>(String.class);
  public static final SlotPrimitivePortability<byte[]> BYTES = new SlotPrimitivePortability<>(byte[].class);

  @SuppressWarnings("unchecked")
  public static <T> SlotPrimitivePortability<T> factory(Class<T> type) {
    if (type == Boolean.class) {
      return (SlotPrimitivePortability<T>) BOOLEAN;
    } else if (type == Byte.class) {
      return (SlotPrimitivePortability<T>) BYTE;
    } else if (type == Character.class) {
      return (SlotPrimitivePortability<T>) CHAR;
    } else if (type == Integer.class) {
      return (SlotPrimitivePortability<T>) INT;
    } else if (type == Long.class) {
      return (SlotPrimitivePortability<T>) LONG;
    } else if (type == Double.class) {
      return (SlotPrimitivePortability<T>) DOUBLE;
    } else if (type == String.class) {
      return (SlotPrimitivePortability<T>) STRING;
    } else if (type == byte[].class) {
      return (SlotPrimitivePortability<T>) BYTES;
    }
    throw new IllegalArgumentException();
  }

  public static class KeyAndSlot<TT> {
    private SlotPrimitivePortability<TT> slotPrimitivePortability;
    private TT key;
    private long slot;

    KeyAndSlot(SlotPrimitivePortability<TT> simplePortability, TT key, long slot) {
      this.key = key;
      this.slot = slot;
      this.slotPrimitivePortability = simplePortability;
    }

    public TT getKey() {
      return key;
    }

    public long getSlot() {
      return slot;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      @SuppressWarnings("unchecked")
      KeyAndSlot<TT> slot1 = (KeyAndSlot<TT>) o;

      return slotPrimitivePortability.compare(this, slot1) == 0;
    }

    @Override
    public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + (int) (slot ^ (slot >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "KeyAndSlot{" +
               "key=" + key +
               ", slot=" + slot +
               '}';
    }
  }

  private PrimitivePortability<T> basePortability;

  private SlotPrimitivePortability(Class<T> type) {
    this.basePortability = PrimitivePortabilityImpl.factory(type);
  }

  public KeyAndSlot<T> keyAndSlot(T key, long slot) {
    return new KeyAndSlot<T>(this, key, slot);
  }

  public PrimitivePortability<T> getBasePortability() {
    return basePortability;
  }

  @Override
  public boolean isInstance(Object o) {
    return o instanceof KeyAndSlot && basePortability.isInstance(((KeyAndSlot) o).getKey());
  }

  @Override
  public void encode(KeyAndSlot<T> slot, ByteBuffer dest) {
    dest.putLong(slot.getSlot());
    basePortability.encode(slot.getKey(), dest);
  }

  @Override
  public KeyAndSlot<T> decode(ByteBuffer buffer) {
    long slot = buffer.getLong();
    T val = basePortability.decode(buffer);
    return new KeyAndSlot<>(this, val, slot);
  }

  @Override
  public ByteBuffer encode(KeyAndSlot<T> slot) {
    int extra = basePortability.spaceNeededToEncode(slot.getKey());
    ByteBuffer b = ByteBuffer.allocate(extra + Long.BYTES);
    b.putLong(slot.getSlot());
    basePortability.encode(slot.getKey(), b);
    b.flip();
    return b;
  }

  @Override
  public boolean equals(Object o, ByteBuffer buffer) {
    if (o instanceof KeyAndSlot) {
      KeyAndSlot<?> ks = (KeyAndSlot<?>) o;
      if (basePortability.isInstance(ks.getKey())) {
        return ks.getKey().equals(decode(buffer));
      }
    }
    return false;
  }

  public int compare(KeyAndSlot<T> o1, KeyAndSlot<T> o2) {
    int ret = basePortability.compare(o1.getKey(), o2.getKey());
    if (ret == 0) {
      ret = Long.compare(o1.getSlot(), o2.getSlot());
    }
    return ret;
  }

  @Override
  public int spaceNeededToEncode(Object obj) {
    return 8 + basePortability.spaceNeededToEncode(obj);
  }

  @Override
  public boolean isFixedSize() {
    return basePortability.isFixedSize();
  }

  @Override
  public Class<KeyAndSlot> getType() {
    return KeyAndSlot.class;
  }
}


