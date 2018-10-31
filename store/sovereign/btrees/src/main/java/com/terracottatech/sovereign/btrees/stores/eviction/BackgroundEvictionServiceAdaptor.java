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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Background eviction service adaptor, with possible live eviction too.
 *
 * @author cschanck
 */
public class BackgroundEvictionServiceAdaptor implements EvictionService {

  private final Timer timer;
  private final EvictionService delegate;
  private final long periodInMs;
  private final boolean timedOnly;
  private TimerTask task = new TimerTask() {
    @Override
    public void run() {
      try {
        delegate.evictAsNeeded(null);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  };

  /**
   * Instantiates a new Background eviction service adaptor.
   *
   * @param service the service
   * @param period the period
   * @param unit the unit
   * @param timedOnly the timed only
   */
  public BackgroundEvictionServiceAdaptor(EvictionService service, int period, TimeUnit unit, boolean timedOnly) {
    this.timedOnly = timedOnly;
    this.timer = new Timer("Background Eviction", true);
    this.delegate = service;

    this.periodInMs = TimeUnit.MILLISECONDS.convert(period, unit);
  }

  @Override
  public EvictionService background(int value, TimeUnit unit, boolean timedOnly) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void begin(int blockSize) {
    delegate.begin(blockSize);
    timer.scheduleAtFixedRate(task, periodInMs, periodInMs);
  }

  @Override
  public void notifyNew(BlockBuffer newBlock) {
    delegate.notifyNew(newBlock);
  }

  @Override
  public void recordBlockFaultIn(BlockBuffer block) {
    delegate.recordBlockFaultIn(block);
  }

  @Override
  public void recordBlockEviction(BlockBuffer block) {
    delegate.recordBlockEviction(block);
  }

  @Override
  public void recordBlockAllocation() {
    delegate.recordBlockAllocation();
  }

  /**
   * Gets active block count.
   *
   * @return the active block count
   */
  @Override
  public int getActiveBlockCount() {
    return delegate.getActiveBlockCount();
  }

  /**
   * Gets allocated block count.
   *
   * @return the allocated block count
   */
  @Override
  public int getAllocatedBlockCount() {
    return delegate.getAllocatedBlockCount();
  }

  /**
   * Gets maximum block count.
   *
   * @return the maximum block count
   */
  @Override
  public int getMaximumBlockCount() {
    return delegate.getMaximumBlockCount();
  }

  /**
   * Gets maximum byte count.
   *
   * @return the maximum byte count
   */
  @Override
  public long getMaximumByteCount() {
    return delegate.getMaximumByteCount();
  }

  @Override
  public void evictAsNeeded(BlockBuffer sentinel) throws IOException {
    if (!timedOnly) {
      delegate.evictAsNeeded(sentinel);
    }
  }

  @Override
  public boolean isHalted() {
    return delegate.isHalted();
  }

  @Override
  public void halt() {
    delegate.halt();
    timer.cancel();
  }

  @Override
  public void recordEvictionRequests() {
    delegate.recordEvictionRequests();
  }

  /**
   * Gets eviction requests.
   *
   * @return the eviction requests
   */
  @Override
  public long getEvictionRequests() {
    return delegate.getEvictionRequests();
  }

  @Override
  public String toString() {
    return "BackgroundEvictionServiceAdaptor{" +
      " periodInMs=" + periodInMs +
      " " + delegate +
      '}';
  }
}
