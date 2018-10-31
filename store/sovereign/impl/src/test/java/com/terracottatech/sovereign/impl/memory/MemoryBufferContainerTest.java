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

import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.Type;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 7/7/2015.
 */
public class MemoryBufferContainerTest {

  private SovereignDataSetConfig<Integer, SystemTimeReference> config;

  @Before
  public void before() {
    this.config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).freeze();
  }

  @Test
  public void testCreate() {
    SovereignRuntime<Integer> r = new SovereignRuntime<>(config, new CachingSequence());
    MemoryBufferContainer cont = new MemoryBufferContainer(new ShardSpec(1, 0), r, new UnlimitedPageSource(new HeapBufferSource()));
    cont.dispose();
  }

  @Test
  public void testAddRetrieve() {
    SovereignRuntime<Integer> r = new SovereignRuntime<>(config, new CachingSequence());
    MemoryBufferContainer cont = new MemoryBufferContainer(new ShardSpec(1, 0), r, new UnlimitedPageSource(new HeapBufferSource()));
    HashMap<Integer, PersistentMemoryLocator> map = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      PersistentMemoryLocator loc1 = cont.add(buffer(i + 1));
      map.put(i, loc1);
    }
    assertThat(cont.count(), is(5l));
    Set<Integer> seen = new HashSet<>();
    for (Integer i : map.keySet()) {
      seen.add(i);
      ByteBuffer buf = cont.get(map.get(i));
      assertNotNull(buf);
    }
    for (Integer ii : seen) {
      map.remove(ii);
    }
    assertThat(map.size(), is(0));
  }

  @Test
  public void testAddIterate() {
    SovereignRuntime<Integer> r = new SovereignRuntime<>(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class),
      new CachingSequence());
    UnlimitedPageSource ps = new UnlimitedPageSource(new HeapBufferSource());
    MemoryRecordContainer<Integer> rcont = new MemoryRecordContainer<>(new ShardSpec(1, 0), r, ps);
    MemoryBufferContainer cont = rcont.getBufferContainer();
    HashMap<Integer, PersistentMemoryLocator> map = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      PersistentMemoryLocator loc1 = cont.add(buffer(i));
      map.put(i, loc1);
    }
    PersistentMemoryLocator loc = cont.first(new ContextImpl(rcont, true));

    Set<Integer> seen = new HashSet<>();
    while (loc.isValid()) {
      ByteBuffer got = cont.get(loc);
      assertNotNull(got);
      int probe = (int) got.get(0);
      seen.add(probe);
      loc = loc.next();
    }

    for (Integer ii : seen) {
      map.remove(ii);
    }

    assertThat(map.size(), is(0));
    rcont.dispose();
  }

  @Test
  public void testAddDelete() throws ExecutionException, InterruptedException {
    SovereignRuntime<Integer> r = new SovereignRuntime<>(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class),
      new CachingSequence());
    UnlimitedPageSource ps = new UnlimitedPageSource(new HeapBufferSource());
    MemoryRecordContainer<Integer> rcont = new MemoryRecordContainer<>(new ShardSpec(1, 0), r, ps);
    MemoryBufferContainer cont = rcont.getBufferContainer();
    HashMap<Integer, PersistentMemoryLocator> map = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      PersistentMemoryLocator loc1 = cont.add(buffer(i));
      map.put(i, loc1);
    }
    ContextImpl c1 = new ContextImpl(rcont, true);
    PersistentMemoryLocator loc = cont.first(c1);
    cont.delete(map.get(0));
    cont.delete(map.get(2));
    cont.delete(map.get(4));

    assertThat(cont.count(), is(2l));
    assertThat(countRemaining(cont, loc), is(2));
    c1.close();

    ContextImpl c2 = new ContextImpl(rcont, true);
    loc = cont.first(c2);
    assertThat(countRemaining(cont, loc), is(2));
    c2.close();

    assertThat(cont.count(), is(2l));
    cont.dispose();
  }

  @Test
  public void testReplace() throws ExecutionException, InterruptedException {
    SovereignRuntime<Integer> r = new SovereignRuntime<>(new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class),
      new CachingSequence());
    UnlimitedPageSource ps = new UnlimitedPageSource(new HeapBufferSource());
    MemoryRecordContainer<Integer> rcont = new MemoryRecordContainer<>(new ShardSpec(1, 0), r, ps);
    MemoryBufferContainer cont = rcont.getBufferContainer();
    HashMap<Integer, PersistentMemoryLocator> map = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      PersistentMemoryLocator loc1 = cont.add(buffer(i));
      map.put(i, loc1);
    }
    ContextImpl c1 = new ContextImpl(rcont, true);
    PersistentMemoryLocator loc = cont.first(c1);
    cont.replace(map.get(2), buffer(10));

    assertThat(cont.count(), is(5l));
    assertThat(countRemaining(cont, loc), is(5));
    c1.close();
    assertThat(cont.count(), is(5l));

    cont.dispose();
  }

  private int countRemaining(MemoryBufferContainer cont, PersistentMemoryLocator loc) {
    int cnt = 0;
    while (loc.isValid()) {
      if (cont.get(loc) != null) {
        cnt++;
      }
      loc = loc.next();
    }
    return cnt;
  }

  private ByteBuffer buffer(int i) {
    ByteBuffer b = ByteBuffer.allocate(100);
    NIOBufferUtils.fill(b, (byte) i);
    return b;
  }

}
