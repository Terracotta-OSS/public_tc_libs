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
package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException.ExtinctionType;
import org.terracotta.offheapstore.buffersource.BufferSource;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author cschanck
 **/
public class SovereignAllocationResource {
  // Java enum caps rules are ignorant and stupid.
  public static enum Type {
    RecordContainer,
    AddressList,
    SortedMap,
    UnsortedMap,
    FRSAddressMap
  }

  private final BufferSource source;
  private final SovereignBufferResource resource;
  private ConcurrentSkipListMap<Type, BufferAllocator> bufferBased = new ConcurrentSkipListMap<>();
  private ConcurrentSkipListMap<Type, PageSourceAllocator> pageSourceBased = new ConcurrentSkipListMap<>();
  private volatile boolean disposed = false;

  public SovereignAllocationResource(SovereignBufferResource resource) {
    this.resource = resource;
    this.source = resource.type() == SovereignBufferResource.MemoryType.OFFHEAP ? new OffHeapBufferSource() : new HeapBufferSource();
  }

  public BufferAllocator getBufferAllocator(Type type) {
    BufferAllocator ret = bufferBased.get(type);
    if (ret != null) {
      return ret;
    }
    BufferAllocator cand = new BufferAllocator(type);
    ret = bufferBased.putIfAbsent(type, cand);
    if (ret != null) {
      return ret;
    }
    return cand;
  }

  public PageSourceAllocator getNamedPageSourceAllocator(Type type) {
    PageSourceAllocator ret = pageSourceBased.get(type);
    if (ret != null) {
      return ret;
    }
    PageSourceAllocator cand = new PageSourceAllocator(type);
    ret = pageSourceBased.putIfAbsent(type, cand);
    if (ret != null) {
      return ret;
    }
    return cand;
  }

  public long allocatedBytes() {
    long ret = bufferBased.values().stream().mapToLong(e -> e.allocatedBytes()).sum();
    return ret + pageSourceBased.values().stream().mapToLong(e -> e.allocatedBytes()).sum();
  }

  public void dispose() {
    if (!disposed) {
      this.disposed = true;
      bufferBased.values().stream().forEach((a) -> { a.dispose(); });
      bufferBased.clear();
      pageSourceBased.values().stream().forEach((a) -> { a.dispose(); });
      pageSourceBased.clear();
    }
  }

  private String allocatedSize() {

    return "[" + resource.getName() + ": " + (resource.getSize() - resource.getAvailable()) + " of " + resource.getSize() + " bytes allocated]";
  }

  @Override
  public String toString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("Sovereign resource allocation: " + MiscUtils.bytesAsNiceString(allocatedBytes()));
    for (Map.Entry<Type, BufferAllocator> e : bufferBased.entrySet()) {
      pw.println("  " + e.getKey() + "'s " + MiscUtils.bytesAsNiceString(e.getValue()
                                                                                 .allocatedBytes()));
    }
    for (Map.Entry<Type, PageSourceAllocator> e : pageSourceBased.entrySet()) {
      pw.println("  " + e.getKey() + "'s " + MiscUtils.bytesAsNiceString(e.getValue()
                                                                                     .allocatedBytes()));
    }
    pw.flush();
    return sw.toString();
  }

  public class BufferAllocator implements BufferSource {
    private final LongAdder allocated = new LongAdder();
    private final Type type;
    private volatile boolean disposed = false;

    public BufferAllocator(Type type) {
      this.type=type;
    }

    public void dispose() {
      if (!disposed) {
        disposed = true;
        resource.free(allocated.longValue());
        allocated.reset();
      }
    }

    /**
     * Here you throw an {@link com.terracottatech.sovereign.exceptions.SovereignExtinctionException}
     * when you can't allocate memory.
     *
     * @param i size
     * @return
     */
    public ByteBuffer allocateBuffer(int i) {
      if (!disposed) {
        if (resource.reserve(i)) {
          ByteBuffer b = source.allocateBuffer(i);
          if (b == null) {
            throw ExtinctionType.MEMORY_ALLOCATION.exception(
                new OutOfMemoryError("BufferAllocator failure - unable to allocate " + i + " bytes from " + resource));
          }
          allocated.add(i);
          return b;
        }
        throw ExtinctionType.RESOURCE_ALLOCATION.exception(new OutOfMemoryError(
          "BufferAllocator failure - unable to reserve " + i + " bytes from " + resource + " " + " " + allocatedSize()));
      }
      throw new IllegalStateException();
    }

    public void freeBuffer(int bufferSize) {
      if (!disposed) {
        allocated.add(0 - bufferSize);
        resource.free(bufferSize);
      }
    }

    public long allocatedBytes() {
      return allocated.longValue();
    }
  }

  public class PageSourceAllocator implements PageSource {
    private final LongAdder allocated = new LongAdder();
    private final Type type;
    private volatile boolean disposed = false;

    public PageSourceAllocator(Type type) {
      this.type=type;
    }

    public long allocatedBytes() {
      return allocated.longValue();
    }

    /**
     * Here you throw an {@link com.terracottatech.sovereign.exceptions.SovereignExtinctionException}
     * when you can't allocate memory.
     *
     * @param size
     * @param thief
     * @param victim
     * @param owner
     * @return
     */
    @Override
    public Page allocate(final int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      if (!disposed) {
        if (resource.reserve(size)) {
          ByteBuffer buffer = source.allocateBuffer(size);
          if (buffer == null) {
            resource.free(size);
            throw ExtinctionType.MEMORY_ALLOCATION.exception(
                new OutOfMemoryError("PageSource failure - unable to allocate " + size + " bytes from " + resource));
          } else {
            allocated.add(size);
            return new Page(buffer, owner);
          }
        }
        throw ExtinctionType.RESOURCE_ALLOCATION.exception(new OutOfMemoryError(
          "PageSource failure - unable to reserve " + size + " bytes from " + resource + " " + allocatedSize()));
      }
      throw new IllegalStateException();
    }

    @Override
    public void free(Page page) {
      if (!disposed) {
        resource.free(page.size());
        allocated.add(0 - page.size());
      }
    }

    public void dispose() {
      if (!disposed) {
        disposed = true;
        resource.free(allocated.longValue());
        allocated.reset();
      }
    }
  }
}
