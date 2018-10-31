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

package com.terracottatech.sovereign.impl.persistence.support;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.persistence.base.OffheapAddressToLSNMap;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.Type;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 5/26/2015.
 */
public class OffheapAddressToLSNMapTest {

  @Test
  public void testCreate() {
    SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).freeze();
    SovereignRuntime<?> r = new SovereignRuntime<>(config, new CachingSequence());
    PageSource ps = new UnlimitedPageSource(new OffHeapBufferSource());
    OffheapAddressToLSNMap lsnmap = new OffheapAddressToLSNMap(r, ps, Integer.MAX_VALUE);
    lsnmap.clear();
  }

  @Test
  public void testInsertion() {
    SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).freeze();
    SovereignRuntime<?> r = new SovereignRuntime<>(config, new CachingSequence());

    PageSource ps = new UnlimitedPageSource(new OffHeapBufferSource());
    OffheapAddressToLSNMap lsnmap = new OffheapAddressToLSNMap(r, ps, Integer.MAX_VALUE);
    for (int i = 0; i < 100; i++) {
      lsnmap.put(i, 100 + i);
    }
    assertThat(lsnmap.size(), is(100l));
    for (int i = 0; i < 100; i++) {
      assertThat((long) lsnmap.getLsn(i), is(i + 100l));
    }
    assertNull(lsnmap.getLsn(101));

    assertThat(lsnmap.firstLSNAndAddress().getKey().longValue(), is(100l));
    assertThat(lsnmap.firstLSNAndAddress().getValue().longValue(), is(0l));

    lsnmap.remove(0l);
    assertThat(lsnmap.firstLSNAndAddress().getKey().longValue(), is(101l));
    assertThat(lsnmap.firstLSNAndAddress().getValue().longValue(), is(1l));
    lsnmap.clear();
  }

  @Test
  public void testInsertionDeletion() {
    SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).freeze();
    SovereignRuntime<?> r = new SovereignRuntime<>(config, new CachingSequence());
    PageSource ps = new UnlimitedPageSource(new OffHeapBufferSource());
    OffheapAddressToLSNMap lsnmap = new OffheapAddressToLSNMap(r, ps, Integer.MAX_VALUE);
    for (int i = 0; i < 100; i++) {
      lsnmap.put(i, 100 + i);
    }
    for (int i = 0; i < 100; i = i + 2) {
      lsnmap.remove(i);
    }

    assertThat(lsnmap.size(), is(50l));
    for (int i = 1; i < 100; i = i + 2) {
      assertThat((long) lsnmap.getLsn(i), is(i + 100l));
    }

    assertThat(lsnmap.firstLSNAndAddress().getKey().longValue(), is(101l));
    assertThat(lsnmap.firstLSNAndAddress().getValue().longValue(), is(1l));

    lsnmap.remove(1l);
    assertThat(lsnmap.firstLSNAndAddress().getKey().longValue(), is(103l));
    assertThat(lsnmap.firstLSNAndAddress().getValue().longValue(), is(3l));

    lsnmap.clear();
  }

  @Test
  public void testInsertionClearInsertion() {
    SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).freeze();
    SovereignRuntime<?> r = new SovereignRuntime<>(config, new CachingSequence());
    PageSource ps = new UnlimitedPageSource(new OffHeapBufferSource());
    OffheapAddressToLSNMap lsnmap = new OffheapAddressToLSNMap(r, ps, Integer.MAX_VALUE);
    for (int i = 0; i < 100; i++) {
      lsnmap.put(i, 100 + i);
    }
    lsnmap.clear();

    assertThat(lsnmap.size(), is(0l));

    for (int i = 0; i < 100; i++) {
      assertNull(lsnmap.getLsn(i));
    }

    for (int i = 0; i < 100; i++) {
      lsnmap.put(i, 10 + i);
    }

    for (int i = 0; i < 100; i++) {
      assertThat(lsnmap.getLsn(i), is(10l + i));
    }

  }

  @Test
  public void testDisposed() throws Exception {
    SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT, SystemTimeReference.class).freeze();
    SovereignRuntime<?> r = new SovereignRuntime<>(config, new CachingSequence());

    PageSource ps = new UnlimitedPageSource(new OffHeapBufferSource());
    OffheapAddressToLSNMap lsnmap = new OffheapAddressToLSNMap(r, ps, Integer.MAX_VALUE);

    lsnmap.put(1, 1);
    lsnmap.dispose();

    assertNull(lsnmap.firstLSNAndAddress());
    assertNull(lsnmap.getLsn(1L));
    assertNull(lsnmap.getAddressForLSN(1L));
    assertThat(lsnmap.size(), is(0L));
    assertNull(lsnmap.verifyInternalConsistency());
  }

}
