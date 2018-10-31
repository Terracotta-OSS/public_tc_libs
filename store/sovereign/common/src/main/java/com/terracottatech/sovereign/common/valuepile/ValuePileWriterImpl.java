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

import com.terracottatech.sovereign.common.utils.StringTool;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author cschanck
 **/
public class ValuePileWriterImpl implements ValuePileWriter {

  private final byte[] skipBytes;
  private OutputStream os;
  private IntArrayList fields = new IntArrayList(64);
  private int totalLength = 0;

  public ValuePileWriterImpl(int bytesToSkip) {
    this.skipBytes = new byte[bytesToSkip];
  }

  public ValuePileWriterImpl(OutputStream os, int bytesToSkip) throws IOException {
    this.skipBytes = new byte[bytesToSkip];
    reset(os);
  }

  @Override
  public void resetCurrent() throws IOException {
    totalLength = 0;
    fields.clear();
    os.write(skipBytes);
  }

  @Override
  public void reset(OutputStream os) throws IOException {
    this.os = os;
    totalLength = 0;
    fields.clear();
    os.write(skipBytes);
  }

  @Override
  public ValuePileWriter bytes(byte[] b, int off, int len) throws IOException {
    os.write(b, off, len);
    totalLength = totalLength + len;
    fields.add(len);
    return this;
  }

  @Override
  public ValuePileWriter oneBoolean(boolean v) throws IOException {
    os.write(v ? (byte) 1 : (byte) 0);
    totalLength = totalLength + 1;
    fields.add(1);
    return this;
  }

  @Override
  public ValuePileWriter oneByte(int v) throws IOException {
    os.write(v);
    totalLength = totalLength + 1;
    fields.add(1);
    return this;
  }

  @Override
  public ValuePileWriter oneShort(int v) throws IOException {
    os.write((v >> 8) & 0xff);
    os.write((v >>> 0) & 0xff);
    fields.add(2);
    totalLength = totalLength + 2;
    return this;
  }

  @Override
  public ValuePileWriter oneChar(int v) throws IOException {
    os.write((v >> 8) & 0xff);
    os.write((v >>> 0) & 0xff);
    fields.add(2);
    totalLength = totalLength + 2;

    return this;
  }

  @Override
  public ValuePileWriter oneInt(int v) throws IOException {
    os.write((v >> 24) & 0xff);
    os.write((v >>> 16) & 0xff);
    os.write((v >>> 8) & 0xff);
    os.write((v >>> 0) & 0xff);
    fields.add(4);
    totalLength = totalLength + 4;
    return this;
  }

  @Override
  public ValuePileWriter encodedInt(int v) throws IOException {
    int len = EncodedInteger.write(v, os);
    fields.add(len);
    totalLength = totalLength + len;
    return this;
  }

  @Override
  public ValuePileWriter oneLong(long v) throws IOException {
    os.write((int) ((v >>> 56) & 0xff));
    os.write((int) ((v >>> 48) & 0xff));
    os.write((int) ((v >>> 40) & 0xff));
    os.write((int) ((v >>> 32) & 0xff));
    os.write((int) ((v >>> 24) & 0xff));
    os.write((int) ((v >>> 16) & 0xff));
    os.write((int) ((v >>> 8) & 0xff));
    os.write((int) ((v >>> 0) & 0xff));
    fields.add(8);
    totalLength = totalLength + 8;
    return this;
  }

  @Override
  public ValuePileWriter oneFloat(float v) throws IOException {
    int ival = Float.floatToIntBits(v);
    oneInt(ival);
    return this;
  }

  @Override
  public ValuePileWriter oneDouble(double v) throws IOException {
    long lval = Double.doubleToLongBits(v);
    oneLong(lval);
    return this;
  }

  @Override
  public ValuePileWriter utfString(String s) throws IOException {
    int sz = StringTool.putEncoded(os, s, s.length());
    fields.add(sz);
    totalLength = totalLength + sz;
    return this;
  }

  @Override
  public int finish() throws IOException {
    // ok, write the tail.
    int sz = 0;
    for (int i = 0; i < fields.size(); i++) {
      sz = sz + EncodedInteger.write(fields.get(i), os);
    }
    // write length at end
    os.write((sz >>> 8) & 0xff);
    os.write((sz >>> 0) & 0xff);
    return totalLength + sz + 2;
  }

  @Override
  public int getFieldCount() {
    return fields.size();
  }

  @Override
  public String toString() {
    return "ValueStackWriterImpl{" + "fields=" + fields + ", totalLength=" + totalLength + '}';
  }
}
