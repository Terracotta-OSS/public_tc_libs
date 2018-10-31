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

package com.terracottatech.sovereign.common.utils;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.impl.SovereignAllocationResource;
import com.terracottatech.sovereign.impl.utils.BitSetAddressList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by cschanck on 5/6/2015.
 */
public class BitSetAddressListTest {

  @Test
  public void testCreation() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 10);
    assertThat(bs.getAllocatedSlots(), is(0l));
  }

  @Test
  public void testSetOne() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 10);
    long slot = bs.add(2);
    assertThat(slot, greaterThanOrEqualTo(0l));
    assertThat(bs.getAllocatedSlots(), is(1024l));
    assertThat(bs.size(), is(1l));
  }

  @Test
  public void testSetOneIllegal() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);

    BitSetAddressList bs = new BitSetAddressList(src, 10);
    try {
      long slot = bs.add(1);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testSetMulti() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 10);
    Map<Long, Long> adrs = new HashMap<>();
    for (int i = 0; i < 2000; i++) {
      long addr = bs.add(i * 2);
      adrs.put(addr, (long) (i * 2));
    }
    assertThat(bs.getAllocatedSlots(), is(2048l));
    assertThat(bs.size(), is(2000l));
    int cnt = 0;
    for (Long addr : adrs.keySet()) {
      cnt++;
      assertThat(bs.get(addr), is(adrs.get(addr)));
    }
    assertThat(cnt, is(2000));
  }

  @Test
  public void testEmptyIterators() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 10);
    assertThat(bs.iterator().hasNext(), is(false));
    long addr = bs.add(10l);
    assertThat(bs.iterator().hasNext(), is(true));
    bs.clear(addr);
    assertThat(bs.iterator().hasNext(), is(false));
  }

  @Test
  public void testSingleIteratorNoGaps() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 10);
    Set<Long> addrs = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      long addr = bs.add(i * 2);
      addrs.add(addr);
    }
    for (long l : bs) {
      assertThat(addrs.remove(l), is(true));
    }
    assertThat(addrs.size(), is(0));
  }

  @Test
  public void testSingleIteratorHoles() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 10);
    List<Long> addrs = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      long addr = bs.add(i * 2);
      addrs.add(addr);
    }
    for (int i = 0; i < addrs.size(); i = i + 2) {
      bs.clear((long) addrs.get(i));
    }
    HashSet<Long> addrset = new HashSet<>();
    for (int i = 1; i < addrs.size(); i = i + 2) {
      addrset.add(addrs.get(i));
    }
    for (long l : bs) {
      assertThat(addrset.remove(l), is(true));
    }
    assertThat(addrset.size(), is(0));
  }

  @Test
  public void testSetMultiBoundary() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
      .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 8);
    for (int i = 0; i < 70000; i++) {
      long addr = bs.add(i * 2);
    }
    assertThat(bs.size(), is(70000l));
    int cnt = 0;
    for (Long addr : bs) {
      cnt++;
      assertThat(bs.get(addr), is(addr << 1));
    }
    assertThat(cnt, is(70000));
  }

  @Test
  public void reserveThenClearThenReserveAChunkAndABit() {
    SovereignAllocationResource.BufferAllocator src = new SovereignAllocationResource(SovereignBufferResource.unlimited())
            .getBufferAllocator(SovereignAllocationResource.Type.AddressList);
    BitSetAddressList bs = new BitSetAddressList(src, 16);

    Set<Long> slots = new HashSet<>();

    bs.reserve(0);
    bs.set(0, 16);
    bs.clear(0);

    int count = 1 << 16;
    for (int i = 0; i < count; i++) {
      long slot = bs.reserve(i);
      bs.set(slot, 16);
      assertTrue(slots.add(slot));
    }

    bs.initializeFreeListByScan();

    long slot = bs.reserve();
    System.out.println("slot: " + slot);
    assertTrue(slots.add(slot));
  }
}
