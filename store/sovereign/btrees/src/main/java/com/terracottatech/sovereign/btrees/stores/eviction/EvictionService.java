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
package com.terracottatech.sovereign.btrees.stores.eviction;

import com.terracottatech.sovereign.btrees.stores.disk.BlockBuffer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The interface for an Eviction service.
 */
public interface EvictionService {

  /**
   * background an eviction service. Code Smell.
   * @param value
   * @param unit
   * @param timedOnly
   * @return
   */
  public EvictionService background(int value, TimeUnit unit, boolean timedOnly);

  /**
   * Begin eviction service.
   *
   * @param blockSize
   */
  public void begin(int blockSize);

  /**
   * Notify of a new Blockbuffer to be managed.
   *
   * @param newBlock
   */
  public void notifyNew(BlockBuffer newBlock);

  /**
   * Record a block fault in.
   *
   * @param block
   */
  public void recordBlockFaultIn(BlockBuffer block);

  /**
   * Record a block eviction.
   * @param block
   */
  public void recordBlockEviction(BlockBuffer block);

  /**
   * Record a new block allocation.
   */
  public void recordBlockAllocation();

  /**
   * Gets active block count.
   *
   * @return the active block count
   */
  public int getActiveBlockCount();

  /**
   * Gets allocated block count.
   *
   * @return the allocated block count
   */
  public int getAllocatedBlockCount();

  /**
   * Gets maximum block count.
   *
   * @return the maximum block count
   */
  public int getMaximumBlockCount();

  /**
   * Gets maximum byte count.
   *
   * @return the maximum byte count
   */
  public long getMaximumByteCount();

  /**
   * Evict as needed, not evicting the sentinel block.
   *
   * @param sentinel
   * @throws IOException
   */
  public void evictAsNeeded(BlockBuffer sentinel) throws IOException;

  /**
   * Is this service halted.
   * @return
   */
  public boolean isHalted();

  /**
   * Halt this service.
   */
  public void halt();

  /**
   * Record a request for eviction.
   */
  public void recordEvictionRequests();

  /**
   * Gets eviction requests.
   *
   * @return the eviction requests
   */
  public long getEvictionRequests();

  /**
   * The type Base.
   *
   * @author cschanck
   */
  public abstract static class Base implements EvictionService {
    private final long maxNumberOfBytes;
    private AtomicInteger activePages = new AtomicInteger(0);
    private AtomicInteger allocatedPages = new AtomicInteger(0);
    private AtomicLong evictionRequests = new AtomicLong(0);
    private volatile boolean halted = false;

    /**
     * Instantiates a new Base.
     *
     * @param maxNumberOfBytes the max number of bytes
     */
    public Base(long maxNumberOfBytes) {
      this.maxNumberOfBytes = maxNumberOfBytes;
    }

    @Override
    public EvictionService background(int value, TimeUnit unit, boolean timedOnly) {
      return new BackgroundEvictionServiceAdaptor(this, value, unit, timedOnly);
    }

    @Override
    public long getMaximumByteCount() {
      return maxNumberOfBytes;
    }

    @Override
    public abstract void begin(int blockSize);

    /**
     * Gets maximum block count.
     *
     * @return the maximum block count
     */
    @Override
    public abstract int getMaximumBlockCount();

    @Override
    public boolean isHalted() {
      return halted;
    }

    @Override
    public void halt() {
      halted = true;
    }

    @Override
    public void notifyNew(BlockBuffer newBlock) {

    }

    @Override
    public String toString() {
      return "EvictionService " + this.getClass().getSimpleName() + " {" +
        " allocatedPages=" + getAllocatedBlockCount() +
        " activePages=" + getActiveBlockCount() +
        " maxPages=" + getMaximumBlockCount() +
        " evictionRequests=" + getEvictionRequests() +
        '}';
    }

    @Override
    public void recordBlockFaultIn(BlockBuffer block) {
      activePages.incrementAndGet();
    }

    @Override
    public void recordBlockEviction(BlockBuffer block) {
      activePages.decrementAndGet();
    }

    @Override
    public void recordBlockAllocation() {
      allocatedPages.incrementAndGet();
    }

    @Override
    public int getActiveBlockCount() {
      return activePages.get();
    }

    @Override
    public int getAllocatedBlockCount() {
      return allocatedPages.get();
    }

    @Override
    public void evictAsNeeded(BlockBuffer sentinel) throws IOException {

    }

    @Override
    public void recordEvictionRequests() {
      evictionRequests.incrementAndGet();
    }

    @Override
    public long getEvictionRequests() {
      return evictionRequests.get();
    }
  }

  /**
   * The type None.
   *
   * @author cschanck
   */
  public static class None extends Base {

    /**
     * Instantiates a new None.
     */
    public None() {
      super(Long.MAX_VALUE);
    }

    @Override
    public EvictionService background(int value, TimeUnit unit, boolean timedOnly) {
      return this;
    }

    @Override
    public void begin(int blockSize) {

    }

    @Override
    public int getMaximumBlockCount() {
      return Integer.MAX_VALUE;
    }
  }

}
