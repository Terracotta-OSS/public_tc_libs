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
package com.terracottatech.sovereign.resource;

import com.terracottatech.sovereign.SovereignBufferResource;

import java.util.concurrent.atomic.AtomicLong;

public class SizedBufferResource implements SovereignBufferResource {
  private final long size;
  private final AtomicLong available;

  public SizedBufferResource(long size) {
    this.size = size;
    this.available = new AtomicLong(size);
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public long getAvailable() {
    return available.get();
  }

  @Override
  public boolean reserve(int bytes) {
    while (true) {
      long currentSize = available.get();
      if (currentSize >= bytes) {
        if (available.compareAndSet(currentSize, currentSize - bytes)) {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  @Override
  public void free(long bytes) {
    available.addAndGet(bytes);
  }

  @Override
  public MemoryType type() {
    return MemoryType.OFFHEAP;
  }

  @Override
  public String toString() {
    return defaultToString();
  }
}
