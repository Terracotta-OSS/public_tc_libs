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
package com.terracottatech.store.server.storage.offheap;

import com.terracottatech.sovereign.SovereignBufferResource;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OffheapBufferResourceTest {
  @Test
  public void isOffheapResource() {
    OffHeapResource offheapResource = new TestOffHeapResource(10);
    OffheapBufferResource bufferResource = new OffheapBufferResource(offheapResource);

    assertEquals(SovereignBufferResource.MemoryType.OFFHEAP, bufferResource.type());
  }

  @Test
  public void allocateUpToLimit() {
    OffHeapResource offheapResource = new TestOffHeapResource(10);
    OffheapBufferResource bufferResource = new OffheapBufferResource(offheapResource);

    assertTrue(bufferResource.reserve(5));
    assertEquals(5, offheapResource.available());
    assertTrue(bufferResource.reserve(3));
    assertEquals(2, offheapResource.available());
    assertTrue(bufferResource.reserve(2));
    assertEquals(0, offheapResource.available());
    assertFalse(bufferResource.reserve(1));
    assertEquals(0, offheapResource.available());
  }

  @Test
  public void rejectNegativeReservations() {
    OffHeapResource offheapResource = new TestOffHeapResource(10);
    OffheapBufferResource bufferResource = new OffheapBufferResource(offheapResource);

    assertTrue(bufferResource.reserve(6));
    assertEquals(4, offheapResource.available());

    try {
      assertTrue(bufferResource.reserve(-3));
      fail("Excepted exception");
    } catch (IllegalArgumentException e) {
      // Test success
    }

    assertEquals(4, offheapResource.available());
  }

  @Test
  public void free() {
    OffHeapResource offheapResource = new TestOffHeapResource(10);
    OffheapBufferResource bufferResource = new OffheapBufferResource(offheapResource);

    assertTrue(bufferResource.reserve(8));
    assertEquals(2, offheapResource.available());
    bufferResource.free(4);
    assertEquals(6, offheapResource.available());
    bufferResource.free(2);
    assertEquals(8, offheapResource.available());
    bufferResource.free(2);
    assertEquals(10, offheapResource.available());
  }

  private static class TestOffHeapResource implements OffHeapResource {
    private final long max;
    private long current;

    public TestOffHeapResource(long max) {
      this.max = max;
    }

    @Override
    public boolean reserve(long count) {
      if (count < 0L) {
        throw new IllegalArgumentException();
      }

      if (current + count > max) {
        return false;
      }

      current += count;

      return true;
    }

    @Override
    public void release(long count) {
      if (count < 0L) {
        throw new IllegalArgumentException();
      }

      current -= count;
    }

    @Override
    public long available() {
      return max - current;
    }

    @Override
    public long capacity() {
      return max;
    }
  }
}
