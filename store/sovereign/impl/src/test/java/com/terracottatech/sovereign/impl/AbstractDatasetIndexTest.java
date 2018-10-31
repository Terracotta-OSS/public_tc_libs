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

import ch.qos.logback.classic.Level;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.indexing.SimpleIndex;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.terracottatech.sovereign.SovereignDataset.Durability.IMMEDIATE;
import static com.terracottatech.store.Cell.cell;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * @author cschanck
 */
public abstract class AbstractDatasetIndexTest {

  private SovereignDatasetImpl<String> dataset;

  @Before
  public abstract void createBucket();

  @After
  public void destroyBucket() {
    dataset.dispose();
    dataset = null;
  }

  public SovereignDatasetImpl<String> getDataset() {
    return dataset;
  }

  public void setDataset(SovereignDataset<String> dataset) {
    this.dataset = (SovereignDatasetImpl<String>) dataset;
  }

  @Test
  public void testMutationOfIndexedCell() throws Exception {
    assertThat(dataset.getConfig().getConcurrency(), greaterThan(1));
    for (int i = 0; i < 1000; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", 10L + i), cell("bar", "value" + i));
    }
    CellDefinition<Long> def = define("foo", LONG);
    dataset.getIndexing().createIndex(def, SovereignIndexSettings.btree()).call();

    // keyN :: {foo, N+10}
    dataset.records().forEach(dataset.applyMutation(SovereignDataset.Durability.LAZY, (rec) -> {
      CellSet cs = new CellSet(rec);
      Optional<Long> opt = cs.get(def);
//      System.out.println("K: " + rec.getKey() + " :: " + rec.get(def).get());
      if (opt.isPresent()) {
        cs.set(def.newCell(opt.get() - 20));
      }
      return cs;
    }));

    dataset.records().forEach((r) -> {
      Optional<Long> val = r.get(def);
      if (val.isPresent()) {
        long indexVal = Integer.parseInt(r.getKey().substring(3));
        assertThat(val.get(), is(indexVal - 10));
      }
    });

  }

  @Test
  public void testBasicIndexManipulation() throws Exception {
    assertThat(dataset.getIndexing().getIndexes().size(), is(0));

    for (int i = 0; i < 10; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", 10L + i), cell("bar", "value" + i));
    }
    dataset.getIndexing().createIndex(define("foo", LONG), SovereignIndexSettings.btree()).call();
    assertThat(dataset.getIndexing().getIndexes().size(), is(1));

    SovereignIndexing indexing = dataset.getIndexing();

    assertThat(indexing.getIndexes().stream().filter(r -> r.on().name().equals("foo")).count(), is(1L));

    SovereignIndex<String> foobarIndex = dataset.getIndexing().createIndex(define("foobar", STRING),
                                                                   SovereignIndexSettings.btree()).call();
    assertThat(dataset.getIndexing().getIndexes().size(), is(2));
    assertThat(indexing.getIndexes().stream().filter(r -> r.on().name().equals("foobar")).count(), is(1L));
    assertThat(indexing.getIndexes().stream().filter(r -> r.definition().isSorted()).count(), is(2L));
    assertThat(indexing.getIndexes().stream().filter(r -> !r.definition().isSorted()).count(), is(0L));

    assertThat(dataset.getIndexing().getIndex(define("foo", LONG), SovereignIndexSettings.btree()), notNullValue());
    List<SovereignIndex<?>> got = dataset.getIndexing().getIndexes();
    dataset.getIndexing().destroyIndex(foobarIndex);
    assertThat(dataset.getIndexing().getIndexes().size(), is(1));
    assertThat(got.iterator().next().on().name(), is("foo"));
  }

  @Test
  public void _populateSecondaryIndex() throws Exception {
    for (int i = 9; i >= 0; i--) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", 10L + i), cell("bar", "value" + i));
    }
    Callable<SovereignIndex<Long>> got = dataset.getIndexing().createIndex(define("foo", LONG), SovereignIndexSettings.btree());
    @SuppressWarnings("unchecked")
    SimpleIndex<Long, String> index = (SimpleIndex<Long, String>) got.call();
    assertThat(index, notNullValue());

    SovereignSortedIndexMap<?, ?> sortedMap = (SovereignSortedIndexMap) index.getUnderlyingIndex();
    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator first = sortedMap.first(c);
      SovereignDatasetImpl<String> sovDataset = dataset;
      long probe = 10l;
      while (!first.isEndpoint()) {
        Record<String> rec = sovDataset.getContainer().get(first);
        Long cv = rec.get(define("foo", LONG)).get();
        Assert.assertThat(probe, is(cv.longValue()));
        first = first.next();
        probe++;
      }
    }
  }

  @Test
  public void _incrementalSecondaryIndex() throws Exception {
    Callable<SovereignIndex<Long>> got = dataset.getIndexing().createIndex(define("foo", LONG),
      SovereignIndexSettings.btree());
    @SuppressWarnings("unchecked")
    SimpleIndex<Long, String> index = (SimpleIndex<Long, String>) got.call();
    assertThat(index, notNullValue());

    for (int i = 9; i >= 0; i--) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", 10L + i), cell("bar", "value" + i));
    }

    SovereignSortedIndexMap<Long, String> sortedMap = (SovereignSortedIndexMap<Long, String>) index.getUnderlyingIndex();
    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator first = sortedMap.first(c);
      SovereignDatasetImpl<String> sovDataset = dataset;
      long probe = 10l;
      while (!first.isEndpoint()) {
        Record<String> rec = sovDataset.getContainer().get(first);
        Long cv = rec.get(define("foo", LONG)).get();
        Assert.assertThat(probe, is(cv.longValue()));
        first = first.next();
        probe++;
      }
    }
  }

  @Test
  public void _mixedLoadSecondaryIndex() throws Exception {
    int i = 0;
    while (i < 100) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", (long) i), cell("bar", "value" + i));
      i++;
    }

    Callable<SovereignIndex<Long>> got = dataset.getIndexing().createIndex(define("foo", LONG), SovereignIndexSettings.btree());
    @SuppressWarnings("unchecked")
    SimpleIndex<Long, String> index = (SimpleIndex<Long, String>) got.call();

    Assert.assertThat(index.isLive(), is(true));

    while (i < 200) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", (long) +i), cell("bar", "value" + i));
      i++;
    }

    SovereignSortedIndexMap<Long, String> sortedMap = (SovereignSortedIndexMap<Long, String>) index.getUnderlyingIndex();
    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator first = sortedMap.first(c);
      SovereignDatasetImpl<String> sovDataset = dataset;
      long probe = 0l;
      while (!first.isEndpoint()) {
        Record<String> rec = sovDataset.getContainer().get(first);
        Long cv = rec.get(define("foo", LONG)).get();
        Assert.assertThat(probe, is(cv.longValue()));
        first = first.next();
        probe++;
      }
      assertThat(probe, is(200L));
    }
  }

  @Test
  public void _dupsInSecondaryIndex() throws Exception {
    int i = 0;
    while (i < 10) {
      dataset.add(IMMEDIATE, "key0" + i, cell("foo", (long) i), cell("bar", "value" + i));
      dataset.add(IMMEDIATE, "key1" + i, cell("foo", (long) i), cell("bar", "value" + 2 * i));
      dataset.add(IMMEDIATE, "key2" + i, cell("foo", (long) i), cell("bar", "value" + 2 * i));
      i++;
    }

    Callable<SovereignIndex<Long>> got = dataset.getIndexing().createIndex(define("foo", LONG), SovereignIndexSettings.btree());
    @SuppressWarnings("unchecked")
    SimpleIndex<Long, String> index = (SimpleIndex<Long, String>) got.call();

    SovereignSortedIndexMap<Long, String> sortedMap = (SovereignSortedIndexMap<Long, String>) index.getUnderlyingIndex();
    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.higher(c, 2l);
      SovereignDatasetImpl<String> sovDataset = dataset;
      long probe = 3l;
      int tick = 0;
      while (!loc.isEndpoint()) {
        Record<String> rec = sovDataset.getContainer().get(loc);
        Long cv = rec.get(define("foo", LONG)).get();
        Assert.assertThat(probe, is(cv.longValue()));
        if (++tick == 3) {
          tick = 0;
          probe++;
        }
        loc = loc.next();
      }
    }
  }

  @Test
  public void _concurrentLoadSecondaryIndex() throws Exception {
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);
    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicInteger last = new AtomicInteger(0);
    final AtomicReference<Exception> exc = new AtomicReference<>(null);
    Thread load = new Thread() {
      @Override
      public void run() {
        try {
          int i = 0;
          while (!done.get()) {
            dataset.add(IMMEDIATE, "key" + i, cell("foo", (long) i), cell("bar", "value" + i));
            last.set(i);
            i++;
          }
          // some more.
          for (int j = 0; j < 10; j++) {
            dataset.add(IMMEDIATE, "key" + i, cell("foo", (long) i), cell("bar", "value" + i));
            last.set(i);
            i++;
          }
        } catch (Exception e) {
          e.printStackTrace();
          exc.set(e);
        }
      }
    };
    load.setDaemon(true);
    load.start();
    Thread.sleep(200);

    LongCellDefinition foo = defineLong("foo");
    dataset.getIndexing().createIndex(foo, SovereignIndexSettings.btree()).call();
    done.set(true);
    load.join();
    assertThat(exc.get(), nullValue());

    long[] probe = {0};
    dataset.records()
        .explain(sp -> assertThat(sp.isSortedCellIndexUsed(), is(true)))
        .filter(foo.value().isGreaterThanOrEqualTo(0L)).forEach(rec -> {
          Long cv = rec.get(foo).get();
          assertThat(probe[0], is(cv.longValue()));
          probe[0]++;
        });
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);
  }

  private void checkRest(SovereignDatasetImpl<String> sovDataset, CellDefinition<String> def,
                         PersistentMemoryLocator loc, String strchars) {
    for (int i = 0; i < strchars.length(); i++) {
      assertThat(loc.isEndpoint(), is(false));
      assertThat(sovDataset.getContainer().get(loc).get(def).get(), is(strchars.charAt(i) + ""));
      loc = loc.next();
    }
    assertThat(loc.isEndpoint(), is(true));
  }

  @Test
  public void testIndexFirstDups() throws Exception {
    String[] vals = { "A", "A", "B", "B", "C", "C", "D", "D", "E", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.first(c);

      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key0", "key1"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key0", "key1"));
      checkRest(sovDataset, fooDef, loc, "ABBCCDDEE");
    }
  }

  @Test
  public void testIndexLastDups() throws Exception {
    String[] vals = { "A", "A", "B", "B", "C", "C", "D", "D", "E", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.last(c);

      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key8", "key9"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key8", "key9"));
      checkRest(sovDataset, fooDef, loc, "EDDCCBBAA");
    }
  }

  @Test
  public void testIndexGreaterThanDups() throws Exception {
    String[] vals = { "A", "A", "B", "B", "C", "C", "D", "D", "E", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.higher(c, "B");

      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key4", "key5"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key4", "key5"));

      checkRest(sovDataset, fooDef, loc, "CDDEE");

      loc = sortedMap.higher(c, "E");
      assertThat(loc.isEndpoint(), is(true));
    }
  }

  @Test
  public void testIndexLessThanDups() throws Exception {
    String[] vals = { "A", "A", "B", "B", "C", "C", "D", "D", "E", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.lower(c, "C");

      String k1 = sovDataset.getContainer().get(loc).getKey();
      assertThat(k1, isOneOf("key2", "key3"));
      loc = loc.next();
      String k2 = sovDataset.getContainer().get(loc).getKey();
      assertThat(k2, isOneOf("key3", "key2"));
      assertThat(k1, not(k2));
      checkRest(sovDataset, fooDef, loc, "BAA");

      loc = sortedMap.lower(c, "A");
      assertThat(loc.isEndpoint(), is(true));
    }
  }

  @Test
  public void testIndexGreaterThanEqualDups() throws Exception {
    String[] vals = { "A", "A", "B", "B", "C", "C", "D", "D", "E", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.get(c, "B");

      String k1 = sovDataset.getContainer().get(loc).getKey();
      loc = loc.next();
      String k2 = sovDataset.getContainer().get(loc).getKey();
      assertThat(k2, not(k1));
      assertThat(k1, isOneOf("key2", "key3"));
      assertThat(k2, isOneOf("key2", "key3"));

      checkRest(sovDataset, fooDef, loc, "BCCDDEE");

      loc = sortedMap.get(c, "E");
      assertThat(loc.isEndpoint(), is(false));
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key8"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key9"));
      checkRest(sovDataset, fooDef, loc, "E");
    }
  }

  @Test
  public void testIndexLessThanEqualDups() throws Exception {
    String[] vals = { "A", "A", "B", "B", "C", "C", "D", "D", "E", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.lowerEqual(c, "C");

      String k1 = sovDataset.getContainer().get(loc).getKey();
      loc = loc.next();
      String k2 = sovDataset.getContainer().get(loc).getKey();
      assertThat(k1, not(k2));
      assertThat(k1, isOneOf("key5", "key4"));
      assertThat(k2, isOneOf("key5", "key4"));

      checkRest(sovDataset, fooDef, loc, "CBBAA");

      loc = sortedMap.lowerEqual(c, "A");
      assertThat(loc.isEndpoint(), is(false));
      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key0", "key1"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), isOneOf("key1", "key0"));

      checkRest(sovDataset, fooDef, loc, "A");
    }
  }

  @Test
  public void testIndexFirst() throws Exception {
    String[] vals = { "A", "B", "C", "D", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.first(c);

      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key0"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key1"));
      checkRest(sovDataset, fooDef, loc, "BCDE");
    }
  }

  @Test
  public void testIndexLast() throws Exception {
    String[] vals = { "A", "B", "C", "D", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.last(c);

      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key4"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key3"));
      checkRest(sovDataset, fooDef, loc, "DCBA");
    }
  }

  @Test
  public void testIndexGreaterThan() throws Exception {
    String[] vals = { "A", "B", "C", "D", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.higher(c, "B");

      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key2"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key3"));

      checkRest(sovDataset, fooDef, loc, "DE");

      loc = sortedMap.higher(c, "E");
      assertThat(loc.isEndpoint(), is(true));
    }
  }

  @Test
  public void testIndexLessThan() throws Exception {
    String[] vals = { "A", "B", "C", "D", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.lower(c, "C");

      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key1"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key0"));

      checkRest(sovDataset, fooDef, loc, "A");

      loc = sortedMap.lower(c, "A");
      assertThat(loc.isEndpoint(), is(true));
    }
  }

  @Test
  public void testIndexGreaterThanEqual() throws Exception {
    String[] vals = { "A", "B", "C", "D", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.get(c, "B");

      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key1"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key2"));

      checkRest(sovDataset, fooDef, loc, "CDE");

      loc = sortedMap.get(c, "E");
      assertThat(loc.isEndpoint(), is(false));
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key4"));
      checkRest(sovDataset, fooDef, loc, "E");
    }
  }

  @Test
  public void testIndexLessThanEqual() throws Exception {
    String[] vals = { "A", "B", "C", "D", "E", };
    CellDefinition<String> fooDef = define("foo", Type.STRING);
    @SuppressWarnings("unchecked")
    SimpleIndex<String, String> index = (SimpleIndex<String, String>) dataset.getIndexing().createIndex(fooDef, SovereignIndexSettings.btree()).call();

    for (int i = 0; i < vals.length; i++) {
      dataset.add(IMMEDIATE, "key" + i, cell("foo", vals[i]));
    }

    SovereignSortedIndexMap<String, String> sortedMap = (SovereignSortedIndexMap<String, String>) index.getUnderlyingIndex();
    SovereignDatasetImpl<String> sovDataset = dataset;

    try (ContextImpl c = new ContextImpl(null, false)) {
      PersistentMemoryLocator loc = sortedMap.lowerEqual(c, "C");

      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key2"));
      loc = loc.next();
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key1"));

      checkRest(sovDataset, fooDef, loc, "BA");

      loc = sortedMap.lowerEqual(c, "A");
      assertThat(loc.isEndpoint(), is(false));
      assertThat(sovDataset.getContainer().get(loc).getKey(), is("key0"));

      checkRest(sovDataset, fooDef, loc, "A");
    }
  }
}
