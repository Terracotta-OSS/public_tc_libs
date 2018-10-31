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
import java.nio.CharBuffer;
import java.util.Arrays;

/**
 * @author cschanck
 */
public final class SingleDataByteBuffer implements DataBuffer {

  private final ByteBuffer buf;

  /**
   * Instantiates a new Single data byte buffer.
   *
   * @param b the b
   */
  public SingleDataByteBuffer(ByteBuffer b) {
    this.buf = b.slice();
  }

  public ByteBuffer delegate() {
    return buf;
  }

  @Override
  public int size() {
    return buf.capacity();
  }

  @Override
  public byte get(int index) {
    return buf.get(index);
  }

  @Override
  public DataBuffer put(int index, byte b) {
    buf.put(index, b);
    return this;
  }

  /**
   * Gets char.
   *
   * @param index the index
   * @return the char
   */
  @Override
  public char getChar(int index) {
    return buf.getChar(index);
  }

  @Override
  public DataBuffer putChar(int index, char c) {
    buf.putChar(index, c);
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
    return buf.getShort(index);
  }

  @Override
  public DataBuffer putShort(int index, short value) {
    buf.putShort(index, value);
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
    return buf.getInt(index);
  }

  @Override
  public DataBuffer putInt(int index, int value) {
    buf.putInt(index, value);
    return this;
  }

  /**
   * Gets float.
   *
   * @param index the index
   * @return the float
   */
  @Override
  public float getFloat(int index) {
    return buf.getFloat(index);
  }

  @Override
  public DataBuffer putFloat(int index, float value) {
    buf.putFloat(index, value);
    return this;
  }

  /**
   * Gets long.
   *
   * @param index the index
   * @return the long
   */
  @Override
  public long getLong(int index) {
    return buf.getLong(index);
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
    ByteBuffer tmp = buf.duplicate();
    tmp.position(index);
    CharBuffer cb = tmp.slice().asCharBuffer();
    cb.get(dest, start, length);
    return this;
  }

  @Override
  public DataBuffer putString(CharSequence src, int start, int end, int index) {
    ByteBuffer tmp = buf.duplicate();
    tmp.position(index);
    CharBuffer cb = tmp.slice().asCharBuffer();
    cb.append(src, start, end);
    return this;
  }

  @Override
  public DataBuffer putLong(int index, long value) {
    buf.putLong(index, value);
    return this;
  }

  /**
   * Gets double.
   *
   * @param index the index
   * @return the double
   */
  @Override
  public double getDouble(int index) {
    return buf.getDouble(index);
  }

  @Override
  public DataBuffer putDouble(int index, double value) {
    buf.putDouble(index, value);
    return this;
  }

  @Override
  public DataBuffer copyWithin(int fromIndex, int toIndex, int many) {
    // TODO use copy within fixup
    ByteBuffer dest = (ByteBuffer) buf.duplicate().clear().position(toIndex);
    ByteBuffer src = (ByteBuffer) buf.duplicate().position(fromIndex).limit(fromIndex + many);
    dest.put(src);
    return this;
  }

  @Override
  public DataBuffer put(byte[] barr, int off, int many, int toIndex) {
    ByteBuffer dest = (ByteBuffer) buf.duplicate().position(toIndex).limit(toIndex + many);
    dest.put(barr, off, many);
    return this;
  }

  @Override
  public DataBuffer get(int fromIndex, byte[] barr, int off, int many) {
    ByteBuffer src = (ByteBuffer) buf.duplicate().clear().position(fromIndex);
    src.get(barr, off, many);
    return this;
  }

  @Override
  public DataBuffer put(ByteBuffer b, int toIndex) {
    ByteBuffer dest = (ByteBuffer) buf.duplicate().position(toIndex).limit(buf.capacity());
    dest.put(b);
    return this;
  }

  @Override
  public DataBuffer get(int fromIndex, ByteBuffer b) {
    ByteBuffer src = (ByteBuffer) buf.duplicate().position(fromIndex).limit(fromIndex + b.limit() - b.position());
    b.put(src);
    return this;
  }

  @Override
  public void fill(byte b) {
    fill(0, size(), b);
  }

  @Override
  public void fill(int off, int size, byte value) {
    byte[] b = new byte[128];
    if (value != 0) {
      Arrays.fill(b, value);
    }
    while (size > 0) {
      int m = Math.min(b.length, size);
      put(b, 0, m, off);
      size = size - m;
      off = off + m;
    }
  }

  @Override
  public DataBuffer put(int destIndex, DataBuffer buffer, int srcIndex, int many) {
    if (buffer instanceof SingleDataByteBuffer) {
      // TODO copy within
      SingleDataByteBuffer b = (SingleDataByteBuffer) buffer;
      ByteBuffer src = b.buf.duplicate();
      src.position(srcIndex).limit(srcIndex + many);
      ByteBuffer dest = buf.duplicate();
      dest.position(destIndex);
      dest.put(src);
    } else {
      while (many-- > 0) {
        buffer.put(destIndex++, buffer.get(srcIndex++));
      }
    }
    return this;
  }
}
