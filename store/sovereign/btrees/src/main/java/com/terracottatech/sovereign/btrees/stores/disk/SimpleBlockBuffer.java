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
package com.terracottatech.sovereign.btrees.stores.disk;

import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.common.utils.MiscUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Simple block buffer, basic non evictable implementation of the BlockBuffer interface.
 *
 * @author cschanck
 */
public class SimpleBlockBuffer implements BlockBuffer {

  protected final int baseAddress;

  protected final BlockBufferFactory factory;

  protected final FileChannel channelProvider;

  protected volatile ByteBuffer buf;
  private final int id;
  private volatile boolean dirty = false;
  private int[] freeList = null;
  private int freeCounter;

  /**
   * Instantiates a new Simple block buffer.
   *
   * @param id              the id
   * @param factory         the factory
   * @param channelProvider the channel provider
   * @param baseAddress     the base address
   * @throws IOException the iO exception
   */
  public SimpleBlockBuffer(int id, BlockBufferFactory factory, FileChannel channelProvider,
                           int baseAddress) throws IOException {
    this(id, factory, channelProvider, baseAddress,
      factory.getDiskProvider().provisionBuffer(factory.getPageSourceLocation(), channelProvider, baseAddress,
        factory.getBlockSize()));
  }

  /**
   * Instantiates a new Simple block buffer.
   *
   * @param id              the id
   * @param factory         the factory
   * @param channelProvider the channel provider
   * @param baseAddress     the base address
   * @param initBuf         the init buf
   * @throws IOException the iO exception
   */
  public SimpleBlockBuffer(int id, BlockBufferFactory factory, FileChannel channelProvider, int baseAddress,
                           ByteBuffer initBuf) throws IOException {
    this.factory = factory;
    this.id = id;
    this.channelProvider = channelProvider;
    this.baseAddress = baseAddress;
    this.buf = initBuf;
    this.freeCounter = 0;
  }

  public int readInt(int offset) throws IOException {
    return buf.getInt(offset);
  }

  public void read(int offset, ByteBuffer dest) throws IOException {
    ByteBuffer b = buf.duplicate();
    b.position(offset);
    b.limit(offset + dest.remaining());
    dest.put(b);
  }

  public void writeInt(int offset, int value) throws IOException {
    buf.putInt(offset, value);
    dirty = true;
  }

  public void write(int offset, ByteBuffer src) throws IOException {
    ByteBuffer b = buf.duplicate();
    b.clear();
    b.position(offset);
    b.put(src);
    dirty = true;
  }

  @Override
  public ByteBuffer readOnly(int offset, int many) throws IOException {
    ByteBuffer b = buf.duplicate();
    b.position(offset);
    b.limit(offset + many);
    return b.slice().asReadOnlyBuffer();
  }

  public int getBaseAddress() {
    return baseAddress;
  }

  public boolean flush() throws IOException {
    if (dirty) {
      factory.getDiskProvider().flush(channelProvider, buf, baseAddress);
      dirty = false;
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public int getFreePages() {
    return freeCounter;
  }

  public int getId() {
    return id;
  }

  @Override
  public void free(int offset) {
    freeList[freeCounter++] = offset;
  }

  @Override
  public int alloc() {
    return freeList[--freeCounter];
  }

  @Override
  public void initAllocations(int max) {
    this.freeCounter = 0;
    freeList = new int[max];
  }

  @Override
  public String toString() {
    return "SimpleReadWriteBlockBuffer{" +
      "baseAddress=" + baseAddress +
      ", id=" + id +
      ", dirty=" + dirty +
      ", free=" + freeCounter +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleBlockBuffer buffer = (SimpleBlockBuffer) o;

    if (baseAddress != buffer.baseAddress) {
      return false;
    }
    if (id != buffer.id) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = baseAddress;
    result = 31 * result + (factory != null ? factory.hashCode() : 0);
    result = 31 * result + id;
    return result;
  }

  /**
   * The type Factory.
   *
   * @author cschanck
   */
  public static class Factory implements BlockBufferFactory {
    private final PageSourceLocation pageSourceLocation;
    private final DiskBufferProvider diskProvider;
    private final int blockSize;

    /**
     * Instantiates a new Factory.
     *
     * @param bsl          the bsl
     * @param diskProvider the disk provider
     * @param blockSize    the block size
     */
    public Factory(PageSourceLocation bsl, DiskBufferProvider diskProvider, int blockSize) {
      this.pageSourceLocation = bsl;
      this.diskProvider = diskProvider;
      this.blockSize = (int) Math.min(MiscUtils.nextPowerOfTwo(blockSize), 1 << 30);
    }

    @Override
    public int getBlockSize() {
      return blockSize;
    }

    /**
     * Gets buffer source location.
     *
     * @return the buffer source location
     */
    public PageSourceLocation getPageSourceLocation() {
      return pageSourceLocation;
    }

    /**
     * Gets disk provider.
     *
     * @return the disk provider
     */
    public DiskBufferProvider getDiskProvider() {
      return diskProvider;
    }

    @Override
    public BlockBuffer make(int id, FileChannel provider, int offset) throws IOException {
      return new SimpleBlockBuffer(id, this, provider, offset);
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
      return "Factory{" +
        "pageSourceLocation=" + pageSourceLocation +
        ", diskProvider=" + diskProvider +
        ", blockSize=" + blockSize +
        '}';
    }
  }
}

