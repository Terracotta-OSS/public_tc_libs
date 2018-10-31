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
package com.terracottatech.sovereign.common.valuepile;

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.common.utils.StringTool;

import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author cschanck
 **/
public class ValuePileReaderImpl implements ValuePileReader {
  private final int skipBytes;
  private ByteBuffer src;
  private int[] fieldSizes;
  private int[] fieldOffsets;
  private int fieldCount;
  private int nextField;

  public ValuePileReaderImpl(int skip) {
    this.skipBytes = skip;
  }

  public ValuePileReaderImpl(ByteBuffer src, int skip) {
    this.skipBytes = skip;
    reset(src);
  }

  @Override
  public ValuePileReader dup(boolean dupBuffer) {
    ValuePileReaderImpl dup = new ValuePileReaderImpl(skipBytes);
    src.clear();
    nextField = 0;
    if (dupBuffer) {
      dup.src = NIOBufferUtils.dup(src, true);
    } else {
      dup.src = src.duplicate();
    }
    dup.src.position(skipBytes);
    dup.fieldOffsets = Arrays.copyOf(fieldOffsets, fieldCount);
    dup.fieldSizes = Arrays.copyOf(fieldSizes, fieldCount);
    dup.fieldCount = fieldCount;
    dup.nextField = 0;
    return dup;
  }



  @Override
  public int getFieldCount() {
    return fieldCount;
  }

  @Override
  public int getNextFieldIndex() {
    return nextField;
  }

  public void resetCurrent() {
    src.clear();
    this.nextField = 0;
    src.position(skipBytes);
  }

  @Override
  public void reset(ByteBuffer buf) {
    this.src = buf.slice();
    this.src.position(this.src.capacity() - 2);
    int i1 = readNext();
    int i2 = readNext();
    int footerLen = i1 << 8 | i2;
    src.position(this.src.capacity() - 2 - footerLen);
    fieldSizes = new int[this.src.remaining() - 2];
    fieldOffsets = new int[this.src.remaining() - 2];
    int index = 0;
    int offset = 0;
    while (this.src.remaining() > 2) {
      fieldOffsets[index] = offset;
      int sz = EncodedInteger.read(this.src);
      offset = offset + sz;
      fieldSizes[index++] = sz;
    }
    this.fieldCount = index;
    this.src.clear();
    this.nextField = 0;
    this.src.position(skipBytes);
  }

  @Override
  public ValuePileReader skipField() {
    src.position(src.position() + fieldSizes[nextField++]);
    return this;
  }

  private int readNext() {
    return Byte.toUnsignedInt(src.get());
  }

  private ByteBuffer fetchBytes(int len) {
    src.limit(src.position() + len);
    ByteBuffer ret = src.slice();
    src.limit(src.capacity());
    src.position(src.position() + len);
    return ret;
  }

  private short fetchShort() {
    int ret = (readNext() << 8 | readNext());
    return (short) ret;
  }

  private boolean fetchBoolean() {
    int ret = readNext();
    return ret == 0 ? false : true;
  }

  private char fetchChar() {
    int ret = (readNext() << 8 | readNext());
    return (char) ret;
  }

  private long fetchLong() {
    long ret = ((long) readNext()) << 56;
    ret = ret | ((long) readNext()) << 48;
    ret = ret | ((long) readNext()) << 40;
    ret = ret | ((long) readNext()) << 32;
    ret = ret | ((long) readNext()) << 24;
    ret = ret | ((long) readNext()) << 16;
    ret = ret | ((long) readNext()) << 8;
    ret = ret | ((long) readNext());
    return ret;
  }

  private float fetchFloat() {
    int ret = fetchInt();
    return Float.intBitsToFloat(ret);
  }

  private int fetchInt() {
    int ret = readNext() << 24;
    ret = ret | readNext() << 16;
    ret = ret | readNext() << 8;
    ret = ret | readNext();
    return ret;
  }

  private byte fetchByte() {
    int ret = readNext();
    return (byte) ret;
  }

  private double fetchDouble() {
    long ret = fetchLong();
    return Double.longBitsToDouble(ret);
  }

  private String fetchString(int len) {
    try {
      return StringTool.decodeString(src, len);
    } catch (UTFDataFormatException e) {
      throw new RuntimeException(e);
    }
  }

  private int fetchEncodedInt() {
    return EncodedInteger.read(src);
  }

  @Override
  public ByteBuffer bytes() {
    int len = fieldSizes[nextField];
    ByteBuffer ret = fetchBytes(len);
    nextField++;
    return ret;
  }

  @Override
  public int encodedInt() {
    int ret = fetchEncodedInt();
    nextField++;
    return ret;
  }

  @Override
  public boolean oneBoolean() {
    boolean ret = fetchBoolean();
    nextField++;
    return ret;
  }

  @Override
  public byte oneByte() {
    byte ret = fetchByte();
    nextField++;
    return ret;
  }

  @Override
  public short oneShort() {
    short ret = fetchShort();
    nextField++;
    return ret;
  }

  @Override
  public char oneChar() {
    char ret = fetchChar();
    nextField++;
    return ret;
  }

  @Override
  public int oneInt() {
    int ret = fetchInt();
    nextField++;
    return ret;
  }

  @Override
  public long oneLong() {
    long ret = fetchLong();
    nextField++;
    return ret;
  }

  @Override
  public float oneFloat() {
    float ret = fetchFloat();
    nextField++;
    return ret;
  }

  @Override
  public double oneDouble() {
    double ret = fetchDouble();
    nextField++;
    return ret;
  }

  @Override
  public String utfString() {
    int len = fieldSizes[nextField++];
    return fetchString(len);
  }

  private void positionFor(int offset) {
    src.clear();
    src.position(skipBytes + offset);
  }

  @Override
  public ByteBuffer bytes(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchBytes(fieldSizes[fidx]);
  }

  @Override
  public boolean oneBoolean(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchBoolean();
  }

  @Override
  public byte oneByte(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchByte();
  }

  @Override
  public short oneShort(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchShort();
  }

  @Override
  public char oneChar(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchChar();
  }

  @Override
  public int oneInt(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchInt();
  }

  @Override
  public int encodedInt(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchEncodedInt();
  }

  @Override
  public long oneLong(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchLong();
  }

  @Override
  public float oneFloat(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchFloat();
  }

  @Override
  public double oneDouble(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchDouble();
  }

  @Override
  public String utfString(int fidx) {
    positionFor(fieldOffsets[fidx]);
    return fetchString(fieldSizes[fidx]);
  }

  @Override
  public String toString() {
    return "ValueStackReaderImpl{" + "fieldSizes=" + Arrays.toString(fieldSizes) + ", fieldOffsets=" + Arrays.toString(
      fieldOffsets) + '}';
  }
}
