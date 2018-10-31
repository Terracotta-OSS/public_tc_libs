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
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Clock;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Used to examine the operation of the off-heap components.
 *
 * @author Clifford W. Johnson
 */
public class IndexVsNoIndexTest {
  public static final int DATASET_COPIES_TO_LOAD = 10;
  private final int TEST_TIMES = 20;

  static StringCellDefinition nameDef = defineString("name");
  static StringCellDefinition canonNameDef = defineString("canon_name");
  static StringCellDefinition countryDef = defineString("country");
  static StringCellDefinition typeDef = defineString("type");
  private SovereignDataset<String> dataset = null;

  @Before
  public void before() throws Exception {
    dataset = new SovereignBuilder<>(Type.STRING, FixedTimeReference.class).offheap(1024 * 1024 * 1024).limitVersionsTo(
      1).concurrency(2).build();

    load(DATASET_COPIES_TO_LOAD);
  }

  @After
  public void after() throws IOException {
    dataset.getStorage().destroyDataSet(dataset.getUUID());
    dataset = null;
    System.gc();
  }

  @Test
  public void testSingleLookup() throws Exception {
    String target = findMiddleName();
    Predicate<Record<?>> predicate = nameDef.value().is(target);
    runRawIndexComparison(target, predicate,() -> dataset.getIndexing()
      .getIndex(nameDef, SovereignIndexSettings.BTREE)
      .getStatistics()
      .getSortedStatistics()
      .indexGetCount());
  }

  private String findMiddleName() {
    String target;
    long count;
    try (Stream<Record<String>> records = dataset.records()) {
      count = records.count();
    }
    try (Stream<Record<String>> records = dataset.records()) {
      target = records.filter(nameDef.exists())
        .map(r -> r.get(nameDef).get())
        .sorted()
        .skip(count / 2)
        .findFirst()
        .get();
    }
    return target;
  }

  @Test
  public void testLessThan() throws Exception {
    String target = findMinName();
    Predicate<Record<?>> predicate = nameDef.value().isLessThan(target);
    runRawIndexComparison(target, predicate,() -> dataset.getIndexing()
      .getIndex(nameDef, SovereignIndexSettings.BTREE)
      .getStatistics()
      .getSortedStatistics()
      .indexLowerCount());
  }

  @Test
  public void testLessThanEqual() throws Exception {
    String target = findMinName();
    Predicate<Record<?>> predicate = nameDef.value().isLessThanOrEqualTo(target);
    runRawIndexComparison(target, predicate,() -> dataset.getIndexing()
      .getIndex(nameDef, SovereignIndexSettings.BTREE)
      .getStatistics()
      .getSortedStatistics()
      .indexLowerEqualCount());
  }

  private String findMinName() {
    try (Stream<Record<String>> records = dataset.records()) {
      String target = records.filter(nameDef.exists()).map(r -> r.get(nameDef).get()).min(Comparator.comparing(String::toString)).get();
      return target + "zzz";
    }
  }

  @Test
  public void testGreaterThan() throws Exception {
    String target = findMaxName();
    Predicate<Record<?>> predicate = nameDef.value().isGreaterThan(target);
    runRawIndexComparison(target, predicate,() -> dataset.getIndexing()
      .getIndex(nameDef, SovereignIndexSettings.BTREE)
      .getStatistics()
      .getSortedStatistics()
      .indexHigherCount());
  }

  @Test
  public void testGreaterThanEqual() throws Exception {
    String target = findMaxName();
    Predicate<Record<?>> predicate = nameDef.value().isGreaterThanOrEqualTo(target);
    runRawIndexComparison(target, predicate, () -> dataset.getIndexing()
      .getIndex(nameDef, SovereignIndexSettings.BTREE)
      .getStatistics()
      .getSortedStatistics()
      .indexHigherEqualCount());
  }

  private String findMaxName() {
    try (Stream<Record<String>> records = dataset.records()) {
      String target = records.filter(nameDef.exists()).map(r -> r.get(nameDef).get()).max(Comparator.comparing(String::toString))
        .get();
      return target.substring(0, target.length() - 1);
    }
  }

  private void runRawIndexComparison(String target, Predicate<Record<?>> predicate, LongSupplier stat) throws
    Exception {
    runRawIndexComparison(target, predicate, stat, 2);
  }

  private void runRawIndexComparison(String target, Predicate<Record<?>> predicate, LongSupplier stat, int
    minimumImprovement) throws
    Exception {
    System.out.println("Target: " + target);

    long rawElapsed = timedFind(predicate);
    long rawMilliseconds = TimeUnit.MILLISECONDS.convert(rawElapsed, TimeUnit.NANOSECONDS);
    System.out.println("Raw elapsed: " + rawMilliseconds + "ms floor: " + target);

    System.out.println(dataset.getIndexing());

    indexName();
    assertThat(dataset.getIndexing().getIndex(nameDef, SovereignIndexSettings.BTREE).getStatistics().indexAccessCount(),
               is(0l));
    //System.out.println(dataset.getIndexing());

    long indexElapsed = timedFind(predicate);
    long indexMilliseconds = TimeUnit.MILLISECONDS.convert(indexElapsed, TimeUnit.NANOSECONDS);
    System.out.println("Indexed elapsed: " + indexMilliseconds + "ms floor: " + target);
    //System.out.println("-->" + stat.getAsLong());

    if (indexElapsed == 0) {
      if (rawElapsed < minimumImprovement) {
        fail("Sensible time comparison not possible because the test was too fast");
      }
    } else {
      long improvement = rawElapsed / indexElapsed;
      assertThat(indexElapsed * minimumImprovement, lessThan(rawElapsed));
      System.out.println("Index improvement: " + improvement + "X");
    }
    assertThat(dataset.getIndexing().getIndex(nameDef, SovereignIndexSettings.BTREE).getStatistics().indexAccessCount(),
               greaterThan(0l));
    assertThat(stat.getAsLong(), greaterThan(0l));
    //System.out.println(dataset.getIndexing());
  }

  private long indexName() throws Exception {
    System.out.println("Indexing...");
    long now = System.currentTimeMillis();
    dataset.getIndexing().createIndex(nameDef, SovereignIndexSettings.BTREE).call();
    long ret = (System.currentTimeMillis() - now);
    System.out.println("Indexing took " + ret + "ms");
    return ret;
  }

  private long load(int copies) throws IOException {
    long now = System.currentTimeMillis();
    try (final CSVParser parser = CSVFormat.DEFAULT.withHeader().withSkipHeaderRecord().parse(
      new InputStreamReader(IndexVsNoIndexTest.class.getResourceAsStream("cities.csv")))) {
      for (final CSVRecord record : parser) {
        int id = Integer.parseInt(record.get(0));
        String name = record.get(1);
        String canonName = record.get(2);
        String country = record.get(4);
        String type = record.get(5);

        for (int i = 0; i < copies; i++) {
          dataset.add(IMMEDIATE, id + "-" + i, nameDef.newCell(name), canonNameDef.newCell(canonName),
            countryDef.newCell(country), typeDef.newCell(type));
        }
      }
    }

    long ret = (System.currentTimeMillis() - now);
    try (final Stream<Record<String>> recordStream = dataset.records()) {
      System.out.println("Loaded... " + recordStream.count() + " items in " + ret + "ms");
    }
    return ret;
  }

  private long timedFind(Predicate<Record<?>> pred) {
    final AtomicLong count = new AtomicLong(0);
    long nanos = 0;
    for (int i = 0; i < TEST_TIMES; i++) {
      try (Stream<Record<String>> records = dataset.records()) {
        Stream<Record<String>> matchingRecords = records.filter(pred);
        long start = System.nanoTime();
        Optional<Record<String>> opt = matchingRecords.findAny();
        long end = System.nanoTime();
        long elapsed = end - start;
        nanos = nanos + elapsed;
        count.addAndGet(opt.isPresent() ? 1 : 0);
      }
    }

    System.out.println("Hits in " + TEST_TIMES + " attempts: " + count.get());

    return nanos;
  }

  @Test
  @Ignore(value="Does not complete, used for simulation purposes.")
  public void testLoadAndFilterLikeMAandMForGary() throws Exception {
    // 200 records/5 seconds.
    // 40 records/second
    SovereignDatasetImpl<String> ds = (SovereignDatasetImpl<String>) new SovereignBuilder<>(Type.STRING, SystemTimeReference.class).bufferStrategyLazy()
      .concurrency(1).offheap().build();
    LongCellDefinition tsDef = CellDefinition.defineLong("ts");
    StringCellDefinition strDef = CellDefinition.defineString("tmpstr");
    IntCellDefinition varDefs[]=new IntCellDefinition[20];
    for(int i=0;i<varDefs.length;i++) {
      varDefs[i]=CellDefinition.defineInt("int"+i);
    }
    ds.getIndexing().createIndex(tsDef, SovereignIndexSettings.BTREE).call();
    final int INSERTS_PER_SECOND=5000;
    final int INSERTS_PER_100TH_SECOND=INSERTS_PER_SECOND/100;
    final Semaphore fill = new Semaphore(INSERTS_PER_SECOND);
    AtomicBoolean dead = new AtomicBoolean(false);
    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
      int currentCount = INSERTS_PER_100TH_SECOND - fill.availablePermits();
      if (currentCount <= 0) {
        return;
      }
      fill.release(currentCount);
    }, 10, 10, TimeUnit.MILLISECONDS);

    final Clock clk = Clock.systemUTC();
    Thread filler = new Thread(() -> {
      int next=0;
      for (; !dead.get(); ) {
        try {
          fill.acquire();
          ds.add(SovereignDataset.Durability.IMMEDIATE,
                 UUID.randomUUID().toString(),
                 tsDef.newCell(clk.millis()),
                 strDef.newCell("asjhasdfkljhaklsdj fasdjkl haklsjdfahjksdfasdasdg "),
                 varDefs[0].newCell(next++),
                 varDefs[1].newCell(next++),
                 varDefs[2].newCell(next++),
                 varDefs[3].newCell(next++),
                 varDefs[4].newCell(next++),
                 varDefs[5].newCell(next++),
                 varDefs[6].newCell(next++),
                 varDefs[7].newCell(next++),
                 varDefs[8].newCell(next++),
                 varDefs[9].newCell(next++),
                 varDefs[10].newCell(next++));
        } catch (InterruptedException e) {
        }
      }
    });
    filler.start();
    for (int i = 0; i < 1000; i++) {
      try (Stream<Record<String>> stream = ds.records()) {
        Predicate<Record<?>> pred = tsDef.value().isGreaterThan(clk.millis() - 1000);
        long now = System.nanoTime();
        System.out.println("Count=" + stream.filter(pred).count() + " of: "+ds.recordCount()+" in: " + (System.nanoTime() -
          now));
      }
      Thread.sleep(500);
    }
    dead.set(true);
    filler.join();
  }
}
