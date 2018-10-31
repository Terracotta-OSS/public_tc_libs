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
package com.terracottatech.sovereign.common.dumbstruct.buffers;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Scattered data buffer. Treat a set of underlying buffers a single contiguous buffer.
 *
 * @param <T> type of underlying buffer
 * @author cschanck
 */
public class ScatterDataBuffer implements DataBuffer {

  private final DataBuffer[] base;
  private final int size;
  private final int[] offsets;

  /**
   * Instantiates a new Scatter data buffer.
   *
   * @param bufs the bufs
   */
  public ScatterDataBuffer(DataBuffer... bufs) {
    this.base = Arrays.copyOf(bufs, bufs.length);
    this.offsets = new int[bufs.length];
    int i = 0;
    int sz = 0;
    int curr = 0;
    for (DataBuffer b : base) {
      sz = sz + base[i].size();
      this.offsets[i] = curr;
      curr = curr + base[i].size();
      i++;
    }
    this.size = sz;
  }

  private void readCharsAcrossBoundary(int index, char[] dest, int start, int length) {
    for (int i = 0; i < length; i++) {
      dest[i + start] = getChar(index++);
    }
  }

  @Override
  public int size() {
    return size;
  }

  private int which(int index) {
    int pos = Arrays.binarySearch(offsets, index);
    if (pos < 0) {
      return ~pos - 1;
    } else {
      return pos;
    }
  }

  @Override
  public byte get(int index) {
    int p = which(index);
    DataBuffer db = base[p];
    return db.get(index - offsets[p]);
  }

  @Override
  public DataBuffer put(int index, byte b) {
    int p = which(index);
    DataBuffer db = base[p];
    return db.put(index - offsets[p], b);
  }

  /**
   * Gets char.
   *
   * @param index the index
   * @return the char
   */
  @Override
  public char getChar(int index) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 2 <= db.size()) {
      return db.getChar(off);
    } else {
      return (char) (get(index) << 8 | get(index + 1) & 0xff);
    }
  }

  @Override
  public DataBuffer putChar(int index, char c) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 2 <= db.size()) {
      db.putChar(off, c);
    } else {
      put(index, (byte) (c >> 8));
      put(index + 1, (byte) c);
    }
    return this;
  }

  /**
   * Gets short.
   *
   * @param index the index
   * @return the short
   */
  @Override
  public short getShort(int index) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 2 <= db.size()) {
      return db.getShort(off);
    } else {
      return (short) (get(index) << 8 | get(index + 1) & 0xff);
    }
  }

  @Override
  public DataBuffer putShort(int index, short value) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 2 <= db.size()) {
      db.putShort(off, value);
    } else {
      put(index, (byte) (value >> 8));
      put(index + 1, (byte) value);
    }
    return this;
  }

  /**
   * Gets int.
   *
   * @param index the index
   * @return the int
   */
  @Override
  public int getInt(int index) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 4 <= db.size()) {
      return db.getInt(off);
    } else {
      return (getShort(index) << 16 | getShort(index + 2) & 0xffff);
    }
  }

  @Override
  public DataBuffer putInt(int index, int value) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 4 <= db.size()) {
      db.putInt(off, value);
    } else {
      putShort(index, (short) (value >> 16));
      putShort(index + 2, (short) value);
    }
    return this;
  }

  @Override
  public float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  @Override
  public DataBuffer putFloat(int index, float value) {
    return putInt(index, Float.floatToRawIntBits(value));
  }

  /**
   * Gets long.
   *
   * @param index the index
   * @return the long
   */
  @Override
  public long getLong(int index) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 8 <= db.size()) {
      return db.getLong(off);
    } else {
      return ((long) getInt(index)) << 32 | getInt(index + 4) & 0xffffffffL;
    }
  }

  @Override
  public DataBuffer putLong(int index, long value) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + 8 <= db.size()) {
      db.putLong(off, value);
    } else {
      putInt(index, (int) (value >>> 32));
      putInt(index + 4, (int) value);
    }
    return this;
  }

  /**
   * Gets string.
   *
   * @param index  the index
   * @param dest   the dest
   * @param start  the start
   * @param length the length
   * @return the string
   */
  @Override
  public DataBuffer getString(int index, char[] dest, int start, int length) {
    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];
    if (off + (length * 2) < db.size()) {
      db.getString(off, dest, start, length);
    } else {
      readCharsAcrossBoundary(index, dest, start, length);
    }
    return this;
  }

  @Override
  public DataBuffer putString(CharSequence csq, int start, int end, int index) {
    csq = csq.subSequence(start, end);

    int p = which(index);
    DataBuffer db = base[p];
    int off = index - offsets[p];

    if (off + (csq.length() * 2) < db.size()) {
      db.putString(csq, 0, csq.length(), index);
    } else {
      do {
        int remaining = db.size() - off;
        int written;
        if (remaining == 1) {
          written = 1;
          putChar(index, csq.charAt(0));
        } else {
          written = Math.max(csq.length(), remaining / 2);
          db.putString(csq, 0, written, index);
        }
        csq = csq.subSequence(written, csq.length());
        index += written * 2;
        p = which(index);
        db = base[p];
        off = index - offsets[p];
      } while (csq.length() > 0);
    }
    return this;
  }

  @Override
  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  @Override
  public DataBuffer putDouble(int index, double value) {
    return putLong(index, Double.doubleToRawLongBits(value));
  }

  @Override
  public DataBuffer copyWithin(int fromIndex, int toIndex, int many) {
    if (many <= 0) {
      return this;
    }

    if (toIndex <= fromIndex) {

      while (many > 0) {
        int from = which(fromIndex);
        int to = which(toIndex);
        int fi = fromIndex - offsets[from];
        int ti = toIndex - offsets[to];
        int fromRoom = Math.min(many, base[from].size() - fi);
        int toRoom = Math.min(many, base[to].size() - ti);
        int actual = Math.min(fromRoom, toRoom);
        base[to].put(ti, base[from], fi, actual);
        many = many - actual;
        fromIndex = fromIndex + actual;
        toIndex = toIndex + actual;
      }
    } else {
      fromIndex = fromIndex + many;
      toIndex = toIndex + many;

      while (many > 0) {
        int from = which(fromIndex - 1);
        int to = which(toIndex - 1);
        int fi = fromIndex - offsets[from];
        int ti = toIndex - offsets[to];

        int fromRoom = Math.min(many, fi);
        int toRoom = Math.min(many, ti);
        int actual = Math.min(fromRoom, toRoom);

        base[to].put(ti - actual, base[from], fi - actual, actual);
        many = many - actual;
        fromIndex = fromIndex - actual;
        toIndex = toIndex - actual;
      }
    }
    return this;
  }

  @Override
  public DataBuffer put(byte[] barr, int offset, int many, int toIndex) {
    if (many <= 0) {
      return this;
    }
    int p = which(toIndex);
    DataBuffer db = base[p];
    int off = toIndex - offsets[p];
    int m = db.size() - off;
    while (many > 0) {
      int actual = Math.min(many, m);
      db.put(barr, offset, actual, off);
      many = many - actual;
      if (many > 0) {
        toIndex = toIndex + actual;
        offset = offset + actual;
        p++;
        db = base[p];
        off = toIndex - offsets[p];
        m = db.size() - off;
      }
    }
    return this;
  }

  @Override
  public DataBuffer get(int fromIndex, byte[] barr, int offset, int many) {
    if (many <= 0) {
      return this;
    }
    int p = which(fromIndex);
    DataBuffer db = base[p];
    int off = fromIndex - offsets[p];
    int m = db.size() - off;
    while (many > 0) {
      int actual = Math.min(many, m);
      db.get(off, barr, offset, actual);
      many = many - actual;
      if (many > 0) {
        fromIndex = fromIndex + actual;
        offset = offset + actual;
        p++;
        db = base[p];
        off = fromIndex - offsets[p];
        m = db.size() - off;
      }
    }
    return this;
  }

  @Override
  public DataBuffer put(ByteBuffer b, int toIndex) {
    if (b.remaining() <= 0) {
      return this;
    }
    int p = which(toIndex);
    DataBuffer db = base[p];
    int off = toIndex - offsets[p];
    int m = db.size() - off;
    while (b.remaining() > 0) {
      int actual = Math.min(b.remaining(), m);
      int saveLimit = b.limit();
      b.limit(b.position() + actual);
      db.put(b, off);
      b.limit(saveLimit);
      if (b.remaining() >= 0) {
        toIndex = toIndex + actual;
        p++;
        db = base[p];
        off = toIndex - offsets[p];
        m = db.size() - off;
      }
    }
    return this;
  }

  @Override
  public DataBuffer get(int fromIndex, ByteBuffer b) {
    if (b.remaining() <= 0) {
      return this;
    }
    int p = which(fromIndex);
    while (b.remaining() > 0) {
      DataBuffer db = base[p];
      int off = fromIndex - offsets[p];
      int m = db.size() - off;
      int actual = Math.min(b.remaining(), m);
      int saveLimit = b.limit();
      b.limit(b.position() + actual);
      db.get(off, b);
      b.limit(saveLimit);
      fromIndex = fromIndex + actual;
      p++;
    }
    return this;
  }

  @Override
  public void fill(byte b) {
    for (DataBuffer db : base) {
      db.fill(b);
    }
  }

  @Override
  public void fill(int startOffset, int size, byte value) {
    while (size > 0) {
      int p = which(startOffset);
      DataBuffer db = base[p];
      int off = startOffset - offsets[p];
      int m = db.size() - off;
      int actual = Math.min(size, m);
      db.fill(startOffset, actual, value);
      startOffset = startOffset + actual;
      size = size - actual;
    }
  }

  @Override
  public DataBuffer put(int destIndex, DataBuffer buffer, int srcIndex, int many) {
    // TODO efficiently
    while (many-- > 0) {
      put(destIndex++, buffer.get(srcIndex++));
    }
    return this;
  }

}
