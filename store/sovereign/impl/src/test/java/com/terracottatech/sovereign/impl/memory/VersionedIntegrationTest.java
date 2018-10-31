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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.SovereignRecord;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReferenceVersionLimitStrategy;
import com.terracottatech.store.Cell;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.sovereign.time.SystemTimeReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.definition.CellDefinition.define;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 *
 * @author mscott
 */
public class VersionedIntegrationTest extends AbstractMemoryIntegrationTest {

  public VersionedIntegrationTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Override
  @Before
  public void setUp() {
  }

  @Override
  @After
  public void tearDown() {
  }

  @Override
  public SovereignDataset<Integer> createDataset() {
    return new SovereignBuilder<>(Type.INT, FixedTimeReference.class)
             .offheap(10 * 1024 * 1024)
             .recordLockTimeout(30, TimeUnit.SECONDS)
             .concurrency(8)
             .limitVersionsTo(20).build();
  }

  @Test
  public void testVersion1() throws IOException {
    SovereignDataset<Integer> data =
        new SovereignBuilder<>(Type.INT, FixedTimeReference.class).offheap(1024 * 1024).limitVersionsTo(1).build();
    try {
      CellDefinition<Integer> count = define("counter", Type.INT);
      data.add(IMMEDIATE, 1,count.newCell(1));
      for (int x=0;x<10;x++) {
        try {
          data.applyMutation(IMMEDIATE, 1, r -> true, record -> {
            HashMap<String, Cell<?>> map = new HashMap<>();
            for (Cell<?> c : record) {
              map.put(c.definition().name(), c);
            }
            map.put(count.name(), count.newCell(record.get(count).get() + 1));
            return map.values();
          });
        } catch (Exception e) {
          System.out.println("x is " + Integer.toString(x));
          e.printStackTrace();
        }
      }
      assertThat(data.get(1).get(count).get(), equalTo(11));
      assertThat(data.get(1).versions().count(), equalTo(1L));

    } finally {
      data.getStorage().destroyDataSet(data.getUUID());
    }
  }

  @Test
  public void testTimeFilter() throws Exception {
    SovereignDataset<Integer> data =
        new SovereignBuilder<>(Type.INT, SystemTimeReference.class)
            .offheap(1024 * 1024)
            .limitVersionsTo(100)
            .versionLimitStrategy(new SystemTimeReferenceVersionLimitStrategy(1200, TimeUnit.MILLISECONDS))
            .build();
    try {
      CellDefinition<Integer> count = define("counter", Type.INT);
      data.add(IMMEDIATE, 1,count.newCell(1));
      for (int x=0;x<10;x++) {
        try {
          data.applyMutation(IMMEDIATE, 1, r -> true, record -> {
            HashMap<String, Cell<?>> map = new HashMap<>();
            for (Cell<?> c : record) {
              map.put(c.definition().name(), c);
            }
            map.put(count.name(), count.newCell(record.get(count).get() + 1));
            return map.values();
          });
          Thread.sleep(100);
        } catch (Exception e) {
          System.out.println("x is " + Integer.toString(x));
          e.printStackTrace();
        }
      }
      assertThat("version count", data.get(1).versions().count(), lessThanOrEqualTo(11L));
      Thread.sleep(1000);
      assertThat("version count", data.get(1).versions().count(), lessThan(11L));
      data.applyMutation(IMMEDIATE, 1, r -> true, record -> {
        HashMap<String, Cell<?>> map = new HashMap<>();
        for (Cell<?> c : record) {
          map.put(c.definition().name(), c);
        }
        map.put(count.name(), count.newCell(record.get(count).get() + 1));
        return map.values();
      });
      assertThat(((SovereignPersistentRecord<?>)data.get(1)).elements().size(), lessThan(5));

    } finally {
      data.getStorage().destroyDataSet(data.getUUID());
    }
  }

  @Override
  @Test
  public void testVersionGC() throws Exception {
    final SovereignDataset<Integer> dataset = createDataset();
    try {
      final CellDefinition<String> tag = define("tag", Type.STRING);
      final CellDefinition<Integer> count = define("count", Type.INT);
      final CellDefinition<Integer> series = define("series", Type.INT);

      dataset.add(IMMEDIATE, 1, tag.newCell("CJ"), count.newCell(0), series.newCell(1));
      for (int i = 1; i <= 50; i++) {
        final int c = i;
        dataset.applyMutation(IMMEDIATE, 1, r -> true, r->replaceOrAdd(r, count.newCell(c)));
      }
      assertThat(((SovereignPersistentRecord<?>)dataset.get(1)).elements().size(), lessThan(50));
      try (final Stream<Record<Integer>> recordStream = dataset.records()) {
        assertThat("Full dataset:", recordStream.count(), equalTo(1L));
      }
    } finally {
      dataset.getStorage().destroyDataSet(dataset.getUUID());
    }
  }

  @Override
  @Test
  public void testVersioning() throws IOException {
    SovereignDataset<Integer> data = createDataset();
    try {
      CellDefinition<String> tag = define("tag", Type.STRING);
      CellDefinition<Integer> count = define("count", Type.INT);
      CellDefinition<Integer> series = define("series", Type.INT);
      data.add(IMMEDIATE, 1, tag.newCell("CD"), count.newCell(1), series.newCell(1));
      data.add(IMMEDIATE, 2, tag.newCell("CS"), count.newCell(1), series.newCell(2));
      data.add(IMMEDIATE, 3, tag.newCell("AS"), count.newCell(1), series.newCell(3));
      data.add(IMMEDIATE, 4, tag.newCell("CJ"), count.newCell(1), series.newCell(4));
      data.add(IMMEDIATE, 5, tag.newCell("MS"), count.newCell(1), series.newCell(5));
      for (int x=0;x<5;x++) {
        data.applyMutation(IMMEDIATE, 2, r -> true, r->replaceOrAdd(r, count.newCell(r.get(count).get() + 1)));
      }
      SovereignRecord<Integer> uptodate = data.get(2);
      Stream<SovereignRecord<Integer>> versions = uptodate.versions();
      versions.peek(r-> {
          assertThat(r.get(tag).get(), is("CS"));
        }).map(r->r.get(count).get()).reduce(7,(s, d)-> {
          assertThat(d, is(s-1));
          return d;
        }
      );
      data.applyMutation(IMMEDIATE, 2, r -> true, r->remove(r, count));
      assertFalse(data.get(2).get(count).isPresent());
      data.applyMutation(IMMEDIATE, 2, r -> true, r->replaceOrAdd(r, count.newCell(0)));
      assertThat(data.get(2).get(count).get(), is(0));
      for (int x=0;x<30;x++) {
        data.applyMutation(IMMEDIATE, 2, r -> true, r->replaceOrAdd(r, count.newCell(r.get(count).get() + 1)));
      }
      assertThat(data.get(2).versions().count(), is(20L));
      assertThat(data.get(2).get(count).get(), is(30));
      for (int x=0;x<50;x++) {
        data.applyMutation(IMMEDIATE, 2, r -> true, r->replaceOrAdd(r, count.newCell(r.get(count).get() + 1)));
      }
      assertThat(data.get(2).get(count).get(), is(80));
      assertThat(data.get(2).versions().count(), is(20L));

    } finally {
      data.getStorage().destroyDataSet(data.getUUID());
    }
  }
}
