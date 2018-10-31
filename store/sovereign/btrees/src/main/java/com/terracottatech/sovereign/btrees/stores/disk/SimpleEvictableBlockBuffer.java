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

import com.terracottatech.sovereign.btrees.stores.eviction.EvictionService;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The block buffer impl which is evictable.
 *
 * @author cschanck
 */
public class SimpleEvictableBlockBuffer extends SimpleBlockBuffer implements EvictableBlockBuffer {
  private final EvictionService evictionService;
  private long age = 0;
  private int refCount = 0;
  private int modCount = 0;
  private ConcurrentLinkedQueue<ByteBuffer> bufferQueue = new ConcurrentLinkedQueue<ByteBuffer>();
  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private ReentrantLock faultLock = new ReentrantLock();

  /**
   * Instantiates a new Simple evictable block buffer.
   *
   * @param id the id
   * @param factory the factory
   * @param channelProvider the channel provider
   * @param baseAddress the base address
   * @throws IOException the iO exception
   */
  public SimpleEvictableBlockBuffer(int id, EvictableBlockBufferFactory factory, FileChannel channelProvider,
                                    int baseAddress) throws IOException {
    super(id, factory, channelProvider, baseAddress, null);
    this.evictionService = factory.getEvictionService();
    this.evictionService.notifyNew(this);
  }

  private ByteBuffer requestBuffer() {
    ByteBuffer tmp = bufferQueue.poll();
    if (tmp == null) {
      tmp = factory.getPageSourceLocation().allocateBuffer(factory.getBlockSize());
      evictionService.recordBlockAllocation();
    }
    return tmp;
  }

  private void releaseBuffer(ByteBuffer tmp) {
    bufferQueue.add(tmp);
  }

  public void pin() throws IOException {
    if (buf == null) {
      faultLock.lock();
      try {
        if (buf == null) {
          ByteBuffer b = requestBuffer();
          factory.getDiskProvider().provisionBuffer(b, channelProvider, getBaseAddress(), factory.getBlockSize());
          buf = b;
          modCount = 0;
          refCount = 0;
          evictionService.recordBlockFaultIn(this);

        }
      } finally {
        faultLock.unlock();
      }
    }
  }

  @Override
  public boolean isLoaded() {

    return buf != null;
  }

  @Override
  public long getAge() {
    return age;
  }

  @Override
  public void evict() throws IOException {
    if (buf != null) {
      rwLock.writeLock().lock();
      try {
        if (buf != null) {
          if (modCount > 0) {
            factory.getDiskProvider().flush(channelProvider, buf.slice(), baseAddress);
            buf.clear();
            modCount = 0;
          }
          refCount = 0;
          ByteBuffer tmp = buf;
          tmp.clear();
          buf = null;

          releaseBuffer(tmp);
          evictionService.recordBlockEviction(this);

        }
      } finally {
        rwLock.writeLock().unlock();
      }
    }
  }

  @Override
  public int getReferenceCount() {
    return refCount;
  }

  @Override
  public void setReferenceCount(int cnt) {
    refCount = cnt;
  }

  @Override
  public int getModCount() {
    return modCount;
  }

  @Override
  public int readInt(int offset) throws IOException {
    rwLock.readLock().lock();
    int ret = 0;
    try {
      pin();
      ret = super.readInt(offset);
      refCount++;
    } finally {
      rwLock.readLock().unlock();
    }
    evictionService.evictAsNeeded(this);
    return ret;
  }

  @Override
  public void read(int offset, ByteBuffer dest) throws IOException {
    rwLock.readLock().lock();
    int ret = 0;
    try {
      pin();
      super.read(offset, dest);
      refCount++;
    } finally {
      rwLock.readLock().unlock();
    }
    evictionService.evictAsNeeded(this);
  }

  @Override
  public void writeInt(int offset, int value) throws IOException {
    rwLock.readLock().lock();
    try {
      pin();
      super.writeInt(offset, value);
      modCount++;
      refCount++;
    } finally {
      rwLock.readLock().unlock();
    }
    evictionService.evictAsNeeded(this);
  }

  @Override
  public void write(int offset, ByteBuffer src) throws IOException {
    rwLock.readLock().lock();
    try {
      pin();
      super.write(offset, src);
      modCount++;
      refCount++;
    } finally {
      rwLock.readLock().unlock();
    }
    evictionService.evictAsNeeded(this);
  }

  @Override
  public ByteBuffer readOnly(int offset, int many) throws IOException {
    rwLock.readLock().lock();
    ByteBuffer ret;
    try {
      pin();
      ret = super.readOnly(offset, many);
      refCount++;
    } finally {
      rwLock.readLock().unlock();
    }
    evictionService.evictAsNeeded(this);
    return ret;
  }

  @Override
  public boolean flush() throws IOException {
    if (modCount > 0) {
      rwLock.readLock().lock();
      try {
        super.flush();
        modCount = 0;
        return true;
      } finally {
        rwLock.readLock().unlock();
      }
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    evict();
    super.close();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    SimpleEvictableBlockBuffer buffer = (SimpleEvictableBlockBuffer) o;

    if (bufferQueue != null ? !bufferQueue.equals(buffer.bufferQueue) : buffer.bufferQueue != null) {
      return false;
    }
    if (evictionService != null ? !evictionService.equals(buffer.evictionService) : buffer.evictionService != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (evictionService != null ? evictionService.hashCode() : 0);
    result = 31 * result + (bufferQueue != null ? bufferQueue.hashCode() : 0);
    return result;
  }

  /**
   * The type Factory.
   *
   * @author cschanck
   */
  public static class Factory extends SimpleBlockBuffer.Factory implements EvictableBlockBufferFactory {
    private final EvictionService evictionService;

    /**
     * Instantiates a new Factory.
     *
     * @param bsl the bsl
     * @param diskProvider the disk provider
     * @param evictionService the eviction service
     * @param blockSize the block size
     */
    public Factory(PageSourceLocation bsl, DiskBufferProvider diskProvider, EvictionService evictionService,
                   int blockSize) {
      super(bsl, diskProvider, blockSize);
      this.evictionService = evictionService == null ? new EvictionService.None() : evictionService;
      this.evictionService.begin(blockSize);
    }

    @Override
    public BlockBuffer make(int id, FileChannel provider, int offset) throws IOException {
      SimpleEvictableBlockBuffer ret = new SimpleEvictableBlockBuffer(id, this, provider, offset);
      return ret;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Factory factory = (Factory) o;

      if (evictionService != null ? !evictionService.equals(
        factory.evictionService) : factory.evictionService != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return evictionService != null ? evictionService.hashCode() : 0;
    }

    @Override
    public void close() {
      evictionService.halt();
    }

    @Override
    public String toString() {
      return "Factory{" +
        "evictionService=" + evictionService +
        ", " + super.toString() + '}';
    }

    /**
     * Gets eviction service.
     *
     * @return the eviction service
     */
    @Override
    public EvictionService getEvictionService() {
      return evictionService;
    }
  }
}
