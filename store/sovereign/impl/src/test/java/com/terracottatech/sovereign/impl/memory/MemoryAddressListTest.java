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

package com.terracottatech.sovereign.impl.memory;
import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.impl.SovereignAllocationResource;
import com.terracottatech.sovereign.spi.store.Context;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;

import java.lang.reflect.Field;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Clifford W. Johnson
 */
public class MemoryAddressListTest {

  MemoryAddressList addressList = new MemoryAddressList(new SovereignAllocationResource(
    SovereignBufferResource.unlimited()).getBufferAllocator(SovereignAllocationResource.Type.AddressList), r -> {}, 10);

  @Test
  public void testDispose() throws Exception {
    final Field storageField = MemoryAddressList.class.getDeclaredField("storage");
    storageField.setAccessible(true);

    assertNotNull(storageField.get(this.addressList));

    this.addressList.dispose();
  }

  @Test
  public void testDisposedClear() throws Exception {
    this.addressList.dispose();

    try {
      this.addressList.clear();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedClearInt() throws Exception {
    this.addressList.dispose();

    try {
      this.addressList.clear(0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedContains() throws Exception {
    this.addressList.dispose();

    try {
      this.addressList.contains(0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedGet() throws Exception {
    this.addressList.dispose();

    try {
      this.addressList.get(0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedIterator() throws Exception {
    this.addressList.dispose();

    try {

      this.addressList.iterator(new Context() {
        @Override
        public void close() {

        }

        @Override
        public void addCloseable(AutoCloseable c) {

        }

      });
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedPut() throws Exception {
    this.addressList.dispose();

    try {
      this.addressList.put(0);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedToString() throws Exception {
    this.addressList.dispose();

    assertNotNull(this.addressList.toString());
  }

  @Test
  public void testDisposedTrade() throws Exception {
    this.addressList.dispose();

    try {
      this.addressList.tradeInPlace(0, 1);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testSetGetClearGC() {
    MemoryAddressList al = new MemoryAddressList(new SovereignAllocationResource(
      SovereignBufferResource.unlimited()).getBufferAllocator(SovereignAllocationResource.Type.AddressList),r -> {}, 10);
    try {
      assertThat(al.getAllocatedSlots(), is(0l));
      assertThat(al.get(500000), is(-1l));
      long index = al.put(100);
      assertThat(al.get(index), is(100l));
      assertThat(al.getAllocatedSlots(), is(65536l));
      assertThat(al.size(), is(1l));
      assertThat(al.get(index + 1), is(-1l));
      long index2 = al.put(200);
      assertThat(al.get(index2), is(200l));
      al.clear(index);
      al.clear(index2);
      assertThat(al.size(), is(0l));
    } finally {
      al.dispose();
    }
  }

  @Test
  public void testContainsForUnAllotedSlot() {
    assertFalse(this.addressList.contains(20));
  }
}
