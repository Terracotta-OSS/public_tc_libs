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
package com.terracottatech.sovereign;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public interface SovereignBufferResource {


  enum MemoryType {
    HEAP,
    OFFHEAP
  }

  long getSize();

  long getAvailable();

  boolean reserve(int bytes);

  void free(long bytes);

  MemoryType type();

  default String getName() {
    return "<unnamed>";
  }

  default String defaultToString() {
    return getClass().getName().substring(1 + getClass().getName().lastIndexOf('.'))
        + "[" + getName() + "]{type=" + type() + ", capacity=" + getSize() + ", available=" + getAvailable() + '}';
  }

  static SovereignBufferResource unlimited() {
    return unlimited(MemoryType.OFFHEAP);
  }

  static SovereignBufferResource limitedOffheap(final long maxBytes) {
    return limited(MemoryType.OFFHEAP, maxBytes);
  }

  static SovereignBufferResource limited(final MemoryType type, final long maxBytes) {
    final Logger LOG = LoggerFactory.getLogger(SovereignBufferResource.class);
    return new SovereignBufferResource() {
      AtomicLong allocated = new AtomicLong(0l);

      @Override
      public long getSize() {
        return maxBytes;
      }

      @Override
      public long getAvailable() {
        return maxBytes - allocated.get();
      }

      @Override
      public boolean reserve(int bytes) {
        for (; ; ) {
          long probe = allocated.get();
          if (probe + bytes <= maxBytes) {
            if (allocated.compareAndSet(probe, probe + bytes)) {
              return true;
            }
          } else {
            LOG.error("Failure allocating " + bytes + " from resource with size=" + maxBytes + " and " + "allocated=" + allocated);
            return false;
          }
        }
      }

      @Override
      public void free(long bytes) {
        if(allocated.addAndGet(0 - bytes) <0l) {
          throw new IllegalStateException();
        }
      }

      @Override
      public MemoryType type() {
        return type;
      }
    };
  }

  static SovereignBufferResource unlimited(final MemoryType type) {
    return new SovereignBufferResource() {
      @Override
      public String toString() {
        return getClass().getName().substring(1 + getClass().getName().lastIndexOf('.'))
            + "[internal_unlimited]{type=" + type() + ", capacity=<unlimited>, available=<unlimited>";
      }

      @Override
      public long getSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public long getAvailable() {
        return Long.MAX_VALUE;
      }

      @Override
      public boolean reserve(int bytes) {
        return true;
      }

      @Override
      public void free(long bytes) {
      }

      @Override
      public MemoryType type() {
        return type;
      }
    };
  }
}
