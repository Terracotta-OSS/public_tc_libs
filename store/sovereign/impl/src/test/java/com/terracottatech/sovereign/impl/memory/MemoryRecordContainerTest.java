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

import com.terracottatech.sovereign.impl.AbstractDataContainerTest;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 7/2/2015.
 */
public class MemoryRecordContainerTest extends AbstractDataContainerTest<Integer> {

  private SovereignDataSetConfig<Integer, DirectTimeReference> config;

  @Before
  public void before() {
    this.config = new SovereignDataSetConfig<>(Type.INT, DirectTimeReference.class).timeReferenceGenerator(
      new DirectTimeReference.Generator()).freeze();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testAddRetrieve() {
    SovereignRuntime<Integer> r = new SovereignRuntime<>(config, new CachingSequence());
    MemoryRecordContainer<Integer> cont = new MemoryRecordContainer<>(new ShardSpec(1, 0), r,
                                                                      new UnlimitedPageSource(new HeapBufferSource()));
    HashMap<Integer, PersistentMemoryLocator> map = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      VersionedRecord<Integer> rec1 = VersionedRecord.single(i,
                                                             DirectTimeReference.instance(10 + i), 100 + i, Collections.singleton(Cell.cell("foo", 100 + i)));
      PersistentMemoryLocator loc1 = cont.add(rec1);
      map.put(i, loc1);
    }
    assertThat(cont.count(), is(5L));
    for (Integer i : map.keySet()) {
      SovereignPersistentRecord<Integer> rec = cont.get(map.get(i));
      assertNotNull(rec);
      assertThat(rec.getKey(), is(i));
      assertThat(rec.get(CellDefinition.define("foo", Type.INT)).get(), is(100 + i));
      assertThat(rec.getTimeReference(), comparesEqualTo(DirectTimeReference.instance(10 + i)));
      assertThat(rec.getMSN(), is((long) 100 + i));
    }
    cont.dispose();
  }

  @Override
  protected SovereignContainer<Integer> getContainer() {
    SovereignRuntime<Integer> r = new SovereignRuntime<>(config, new CachingSequence());
    return new MemoryRecordContainer<>(new ShardSpec(1, 0), r, new UnlimitedPageSource(new HeapBufferSource()));
  }

  @Override
  protected VersionedRecord<Integer> getValue() {
    return VersionedRecord.single(0, DirectTimeReference.instance(10), 100, Collections.singleton(Cell.cell("foo", 100)));
  }
}
