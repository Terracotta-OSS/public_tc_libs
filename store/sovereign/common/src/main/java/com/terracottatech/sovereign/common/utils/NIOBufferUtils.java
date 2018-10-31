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
package com.terracottatech.sovereign.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.Charset;

/**
 * NIO buffer utils. Primarily tools for copy buffers without disturbing mark and position.
 *
 * @author cschanck
 */
public final class NIOBufferUtils {

  public static final boolean USE_RISKY_BUFFER_MOVE = MiscUtils.getBoolean("niobuffer.copy.strategy.risky",
    riskyOverlapAppearsToWork());

  public static final int DUAL_PIVOT_TINY_SIZE = 27;

  private NIOBufferUtils() {
  }

  public static ByteBuffer dup(ByteBuffer buf) {
    return dup(buf, !buf.isDirect());
  }

  public static ByteBuffer dup(ByteBuffer buf, boolean heap) {
    ByteBuffer b;
    if (heap) {
      b = ByteBuffer.allocate(buf.remaining());
    } else {
      b = ByteBuffer.allocateDirect(buf.remaining());
    }
    NIOBufferUtils.copy(buf, b);
    b.clear();
    return b;
  }

  public static void copy(ByteBuffer src, ByteBuffer dest) {
    copy(src, src.position(), dest, dest.position(), Math.min(src.remaining(), dest.remaining()));
  }

  public static void copy(ByteBuffer src, int srcOffset, ByteBuffer dest, int destOffset, int many) {
    src = src.duplicate();
    dest = dest.duplicate();
    src.position(srcOffset).limit(srcOffset + many);
    dest.position(destOffset).limit(destOffset + many);
    dest.put(src);
  }

  public static void copyWithin(ByteBuffer buf, int srcOffset, int destOffset, int many) {
    if (USE_RISKY_BUFFER_MOVE) {
      copyWithinRisky(buf, srcOffset, destOffset, many);
    } else {
      copyWithinConservative(buf, srcOffset, destOffset, many);
    }
  }

  public static void copyWithinRisky(ByteBuffer buf, int srcOffset, int destOffset, int many) {
    copy(buf, srcOffset, buf, destOffset, many);
  }

  public static void copyWithinConservative(ByteBuffer buf, int srcOffset, int destOffset, int many) {
    byte[] barr = new byte[many];
    ByteBuffer tmp = buf.duplicate();
    tmp.position(srcOffset);
    tmp.get(barr);
    tmp.clear().position(destOffset);
    tmp.put(barr);
  }

  public static boolean riskyOverlapAppearsToWork() {
    ByteBuffer b = ByteBuffer.allocate(4);
    b.put((byte) 0);
    b.put((byte) 1);
    b.put((byte) 2);
    b.put((byte) 3);
    b.clear();
    copyWithinRisky(b, 1, 2, 2);
    // should now be 0 1 1 2
    // if wrong it might be 0 1 1 1
    return (b.get(0) == 0) && (b.get(1) == 1) && (b.get(2) == 1) && (b.get(3) == 2);
  }

  public static String dumpBufferAsHex(ByteBuffer buf) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    dumpBufferAsHex(buf, baos);
    try {
      return baos.toString("UTF8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dumpBufferAsHex(ByteBuffer buf, OutputStream dest) {
    dumpBufferAsHex(buf, buf.position(), buf.limit(), dest);
  }

  public static void dumpBufferAsHex(ByteBuffer buf, int startOffset, int lengthInBytes, OutputStream dest) {

    int lastOffset = startOffset + lengthInBytes - 1;
    lastOffset = Math.min(buf.capacity() - 1, lastOffset);
    startOffset = (startOffset / 16) * 16;

    PrintWriter pw = new PrintWriter(new OutputStreamWriter(dest, Charset.defaultCharset()));
    pw.println(
      "ByteBuffer contents: (pos: " + buf.position() + " limit: " + buf.limit() + " cap: " + buf.capacity() + ")");
    while (startOffset < lastOffset) {
      pw.print(String.format("%06x", startOffset));
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 16; i++) {
        if (startOffset + i < lastOffset) {
          byte value = buf.get(startOffset + i);
          pw.print(String.format(" %02x", value));
          if (value >= 32 && value < 127) {
            sb.append((char) value);
          } else {
            sb.append(".");
          }
        } else {
          pw.print("   ");
        }
      }
      pw.print(" " + sb.toString());
      startOffset = startOffset + 16;
      pw.println();
    }
    pw.flush();
  }

  public static void readFrom(ByteBuffer buf, int pos, byte[] b) {
    buf = buf.duplicate();
    buf.position(pos);
    buf.limit(pos + b.length);
    buf.get(b);
  }

  public static void writeTo(ByteBuffer buf, int pos, byte[] b) {
    buf = buf.duplicate();
    buf.position(pos);
    buf.limit(pos + b.length);
    buf.put(b);
  }

  public static ByteBuffer fill(ByteBuffer buffer, byte val) {
    ByteBuffer b = buffer.slice();
    while (b.hasRemaining()) {
      b.put(val);
    }
    return buffer;
  }

  public static ByteBuffer makeAndFill(byte val, int many) {
    ByteBuffer b = ByteBuffer.allocate(many);
    fill(b, val);
    b.clear();
    return b;
  }

  public static void sortIntBufferInPlace(IntBuffer buf, int count) {
    quickSort(buf, 0, count - 1);
  }

  private static void quickSort(IntBuffer buf, int lowerIndex, int higherIndex) {

    int i = lowerIndex;
    int j = higherIndex;

    int pivot = buf.get(lowerIndex + (higherIndex - lowerIndex) / 2);

    while (i <= j) {

      while (buf.get(i) < pivot) {
        i++;
      }
      while (buf.get(j) > pivot) {
        j--;
      }
      if (i <= j) {
        swap(buf, i, j);

        i++;
        j--;
      }
    }

    if (lowerIndex < j) {
      quickSort(buf, lowerIndex, j);
    }
    if (i < higherIndex) {
      quickSort(buf, i, higherIndex);
    }
  }

  public static void dualPivotQuickSort(IntBuffer a, int left, int right) {
    dualPivotQuickSort(a, left, right, 3);
  }

  private static void dualPivotQuickSort(IntBuffer buffer, int left, int right, int divisions) {
    int length = right - left;

    if (length < DUAL_PIVOT_TINY_SIZE) {
      insertionSort(buffer, left, right);
      return;
    }
    int divs = length / divisions;

    int m1 = left + divs;
    int m2 = right - divs;
    if (m1 <= left) {
      m1 = left + 1;
    }
    if (m2 >= right) {
      m2 = right - 1;
    }
    if (buffer.get(m1) < buffer.get(m2)) {
      swap(buffer, m1, left);
      swap(buffer, m2, right);
    } else {
      swap(buffer, m1, right);
      swap(buffer, m2, left);
    }

    long pivot1 = buffer.get(left);
    long pivot2 = buffer.get(right);
    int less = left + 1;
    int great = right - 1;

    for (int k = less; k <= great; k++) {
      if (buffer.get(k) < pivot1) {
        swap(buffer, k, less++);
      } else if (buffer.get(k) > pivot2) {
        while (k < great && buffer.get(great) > pivot2) {
          great--;
        }
        swap(buffer, k, great--);

        if (buffer.get(k) < pivot1) {
          swap(buffer, k, less++);
        }
      }
    }

    int dist = great - less;

    if (dist < 13) {
      divisions++;
    }
    swap(buffer, less - 1, left);
    swap(buffer, great + 1, right);

    dualPivotQuickSort(buffer, left, less - 2, divisions);
    dualPivotQuickSort(buffer, great + 2, right, divisions);

    if (dist > length - 13 && pivot1 != pivot2) {
      for (int k = less; k <= great; k++) {
        if (buffer.get(k) == pivot1) {
          swap(buffer, k, less++);
        } else if (buffer.get(k) == pivot2) {
          swap(buffer, k, great--);

          if (buffer.get(k) == pivot1) {
            swap(buffer, k, less++);
          }
        }
      }
    }

    if (pivot1 < pivot2) {
      dualPivotQuickSort(buffer, less, great, divisions);
    }
  }

  private static void insertionSort(IntBuffer buffer, int left, int right) {
    for (int i = left + 1; i <= right; i++) {
      for (int j = i; (j > left) && (buffer.get(j) < buffer.get(j - 1)); j--) {
        swap(buffer, j, j - 1);
      }
    }
  }

  private static void swap(IntBuffer a, int m1, int m2) {
    int tmp = a.get(m1);
    a.put(m1, a.get(m2));
    a.put(m2, tmp);
  }

  public static ByteBufferInputStream asInputStream(final ByteBuffer buf) {
    return new ByteBufferInputStream(buf);
  }

  // From http://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream Nifty!
  public static class ByteBufferInputStream extends InputStream {
    private ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer b) {
      this.buf = b;
    }

    public int read() throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }
      return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }

      len = Math.min(len, buf.remaining());
      buf.get(bytes, off, len);
      return len;
    }

    public ByteBufferInputStream reuse(ByteBuffer b) {
      this.buf = b;
      return this;
    }
  }

  public static class ByteBufferOutputStream extends OutputStream {
    private int max;
    private ByteBuffer buf;

    public ByteBufferOutputStream() {
      this(2048);
    }

    public ByteBufferOutputStream(int max) {
      this.max = max;
      buf = ByteBuffer.allocate(max);
    }

    @Override
    public void write(int b) throws IOException {
      ensureRemaining(1);
      buf.put((byte) (0xff & b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      ensureRemaining(len);
      buf.put(b, off, len);
    }

    private void ensureRemaining(int need) {
      if (buf.remaining() < need) {
        int total = buf.capacity();
        do {
          total = total << 1;
          if (total < 0) {
            throw new OutOfMemoryError();
          }
        } while (total - buf.position() < need);
        ByteBuffer tmp = ByteBuffer.allocate(total);
        max = total;
        buf.flip();
        tmp.put(buf);
        buf = tmp;
      }
    }

    public ByteBuffer takeBuffer() {
      buf.flip();
      ByteBuffer tmp = buf.slice();
      buf = null;
      return tmp;
    }

    public ByteBufferOutputStream reuse() {
      buf = ByteBuffer.allocate(max);
      return this;
    }
  }

  public static long bufferToLong(ByteBuffer b) {
    return b.getLong(b.position());
  }

  public static ByteBuffer longToBuffer(long v) {
    ByteBuffer b = ByteBuffer.allocate(Long.BYTES);
    b.putLong(0, v);
    return b;
  }
}
