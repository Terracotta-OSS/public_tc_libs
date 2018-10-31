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
package com.terracottatech.sovereign.btrees.stores.location;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author cschanck
 */
public class PageSourceLocation implements StoreLocation {
  private final PageSource src;
  private final int maxAllocationSize;
  private final long totalSize;

  private ConcurrentHashMap<Ident, Page> useMap = new ConcurrentHashMap<>();

  private static class Ident {
    public Object o;

    public Ident(Object o) {
      this.o = o;
    }

    @Override
    public boolean equals(Object o1) {
      if (this == o1) {
        return true;
      }
      if (o1 == null || getClass() != o1.getClass()) {
        return false;
      }

      Ident ident = (Ident) o1;

      if (o != null ? (o != ident.o) : ident.o != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return o != null ? System.identityHashCode(o) : 0;
    }
  }

  public PageSourceLocation(PageSource src, long totalSize, int maxAllocation) {
    this.src = src;
    this.totalSize = totalSize;
    this.maxAllocationSize = maxAllocation;
  }

  public PageSourceLocation(PageSourceLocation old) {
    this.src = old.getPageSource();
    this.totalSize = old.getTotalSize();
    this.maxAllocationSize = old.getMaxAllocationSize();
  }

  public static PageSourceLocation heap() {
    return new PageSourceLocation(new UnlimitedPageSource(new HeapBufferSource()), Long.MAX_VALUE, 1024 * 1024 * 1024);
  }

  public static PageSourceLocation offheap() {
    return new PageSourceLocation(new UnlimitedPageSource(new OffHeapBufferSource()), Long.MAX_VALUE,
      1024 * 1024 * 1024);
  }

  public static PageSourceLocation heap(long size, int max) {
    UpfrontAllocatingPageSource ps = new UpfrontAllocatingPageSource(new HeapBufferSource(), size, max);
    return new PageSourceLocation(ps, size, max);
  }

  public static PageSourceLocation offheap(long size, int max) {
    UpfrontAllocatingPageSource ps = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), size, max);
    return new PageSourceLocation(ps, size, max);
  }

  public ByteBuffer allocateBuffer(int size) {
    if (size > maxAllocationSize) {
      return null;
    }
    Page p = src.allocate(size, false, false, null);
    if (p == null) {
      return null;
    }
    ByteBuffer buf = p.asByteBuffer();
    useMap.put(new Ident(buf), p);
    return buf;
  }

  public void free(ByteBuffer buf) {
    Page p = useMap.remove(new Ident(buf));
    if (p == null) {
      throw new IllegalStateException("Attempt to free non-allocated buffer");
    }
    src.free(p);
  }

  @Override
  public boolean ensure() throws IOException {
    return false;
  }

  @Override
  public boolean exists() throws IOException {
    return true;
  }

  @Override
  public boolean destroy() throws IOException {
    return false;
  }

  public PageSource getPageSource() {
    return src;
  }

  public int getMaxAllocationSize() {
    return maxAllocationSize;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public void freeAll() {
    for (Page p : useMap.values()) {
      src.free(p);
    }
  }
}
