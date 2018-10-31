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

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.AbstractMap;
import java.util.ArrayList;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class MappedScratchPad implements SortScratchPad<ByteBuffer> {
  private final static int MAX_MAPPED_BYTES = 256 * 1024 * 1024;
  private final FileChannel fc;
  private final int chunkMask;
  private final int chunkShift;
  private int currentPos;
  private ArrayList<ByteBuffer> chunkList = new ArrayList<>();
  private ByteBuffer current = null;
  private long count = 0;
  private long nextSize = 4 * 1024;

  public MappedScratchPad(File f) throws IOException {
    if (f.exists() && f.length() > 0) {
      throw new IllegalArgumentException();
    }
    this.fc = FileChannel.open(f.toPath(), CREATE, TRUNCATE_EXISTING, READ, WRITE);
    this.chunkMask = MAX_MAPPED_BYTES - 1;
    this.chunkShift = Long.bitCount(chunkMask);
    currentPos = MAX_MAPPED_BYTES + 1;
  }

  private ByteBuffer chunkFor(long index) {
    long i = index >>> chunkShift;
    return chunkList.get((int) i);
  }

  private int offsetOf(long index) {
    return (int) (index & chunkMask);
  }

  @Override
  public long ingest(ByteBuffer k, ByteBuffer v) throws IOException {
    ensure(8 + k.remaining() + v.remaining());
    int start = currentPos;
    current.putInt(currentPos, k.remaining());
    currentPos = currentPos + 4;
    current.putInt(currentPos, v.remaining());
    currentPos = currentPos + 4;
    NIOBufferUtils.copy(k, k.position(), current, currentPos, k.remaining());
    currentPos = currentPos + k.remaining();
    NIOBufferUtils.copy(v, v.position(), current, currentPos, v.remaining());
    currentPos = currentPos + v.remaining();
    long ret = chunkList.size() - 1;
    ret = ret << chunkShift;
    ret = ret | (long) start;
    count++;
    return ret;
  }

  private void ensure(int need) throws IOException {
    for (; ; ) {
      if (current != null && currentPos + need < current.capacity()) {
        return;
      }

      long sz = nextSize;
      long next = fc.size();
      fc.write(ByteBuffer.wrap(new byte[1]), fc.size() + nextSize - 1);
      MappedByteBuffer buf = fc.map(FileChannel.MapMode.READ_WRITE, next, sz);
      chunkList.add(buf);
      current = buf;
      currentPos = 0;
      nextSize = Math.min(MAX_MAPPED_BYTES, nextSize * 2);
    }
  }

  private ByteBuffer sliceBuf(ByteBuffer ch, int offset, int ksize) {
    ByteBuffer tmp = ch.duplicate();
    tmp.clear().position(offset).limit(offset + ksize);
    return tmp.slice().asReadOnlyBuffer();
  }

  @Override
  public AbstractMap.Entry<ByteBuffer, ByteBuffer> fetchKV(long addr) throws IOException {
    ByteBuffer ch = chunkFor(addr);
    int offset = offsetOf(addr);
    int ksize = ch.getInt(offset);
    offset = offset + 4;
    int vsize = ch.getInt(offset);
    offset = offset + 4;
    ByteBuffer k = sliceBuf(ch, offset, ksize);
    offset = offset + ksize;
    ByteBuffer v = sliceBuf(ch, offset, vsize);
    return new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(k, v);
  }

  @Override
  public ByteBuffer fetchK(long addr) throws IOException {
    ByteBuffer ch = chunkFor(addr);
    int offset = offsetOf(addr);
    int ksize = ch.getInt(offset);
    offset = offset + 8;
    ByteBuffer k = sliceBuf(ch, offset, ksize);
    return k;
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public void clear() throws IOException {
    chunkList.clear();
    count = 0;
    current = null;
  }

  @Override
  public void dispose() throws IOException {
    clear();
    fc.close();
  }
}
