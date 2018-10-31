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
package com.terracottatech.store.client.indexing;

import org.junit.Test;

import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.terracottatech.store.client.indexing.DatasetEntityProxies.getIndexingDataset;
import static com.terracottatech.store.indexing.Index.Status.BROKEN;
import static com.terracottatech.store.indexing.Index.Status.DEAD;
import static com.terracottatech.store.indexing.Index.Status.INITALIZING;
import static com.terracottatech.store.indexing.Index.Status.LIVE;
import static com.terracottatech.store.indexing.Index.Status.POTENTIAL;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link AggregatedIndex}.
 *
 * @see AggregatedIndexingTest
 */
@SuppressWarnings("Duplicates")
public class AggregatedIndexTest {

  private final StringCellDefinition stringCellDef = CellDefinition.defineString("stringCell");

  @Test
  public void testAllLive() throws Exception {
    Collection<Index<String>> knownIndexes = Arrays.asList(
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE)
    );

    AggregatedIndex<String> index =
        new AggregatedIndex<>(stringCellDef, IndexSettings.btree(), knownIndexes, Collections.emptyList());
    assertThat(index.on(), is(stringCellDef));
    assertThat(index.definition(), is(IndexSettings.btree()));
    assertThat(index.status(), is(LIVE));
  }

  @Test
  public void testOneInitializing() throws Exception {
    Collection<Index<String>> knownIndexes = Arrays.asList(
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), INITALIZING),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE)
    );

    AggregatedIndex<String> index =
        new AggregatedIndex<>(stringCellDef, IndexSettings.btree(), knownIndexes, Collections.emptyList());
    assertThat(index.on(), is(stringCellDef));
    assertThat(index.definition(), is(IndexSettings.btree()));
    assertThat(index.status(), is(INITALIZING));
  }

  @Test
  public void testOnePotential() throws Exception {
    Collection<Index<String>> knownIndexes = Arrays.asList(
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), POTENTIAL),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), INITALIZING)
    );

    AggregatedIndex<String> index =
        new AggregatedIndex<>(stringCellDef, IndexSettings.btree(), knownIndexes, Collections.emptyList());
    assertThat(index.on(), is(stringCellDef));
    assertThat(index.definition(), is(IndexSettings.btree()));
    assertThat(index.status(), is(POTENTIAL));
  }

  @Test
  public void testOneDead() throws Exception {
    Collection<Index<String>> knownIndexes = Arrays.asList(
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), POTENTIAL),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), DEAD)
    );

    AggregatedIndex<String> index =
        new AggregatedIndex<>(stringCellDef, IndexSettings.btree(), knownIndexes, Collections.emptyList());
    assertThat(index.on(), is(stringCellDef));
    assertThat(index.definition(), is(IndexSettings.btree()));
    assertThat(index.status(), is(DEAD));
  }

  @Test
  public void testOneMissing() throws Exception {
    Collection<Index<String>> knownIndexes = Arrays.asList(
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE)
    );

    TestIndexing indexingWithMissing = new TestIndexing();
    List<DatasetEntity<String>> missing = Collections.singletonList(
        getIndexingDataset("first", indexingWithMissing)
    );

    AggregatedIndex<String> index =
        new AggregatedIndex<>(stringCellDef, IndexSettings.btree(), knownIndexes, missing);
    assertThat(index.on(), is(stringCellDef));
    assertThat(index.definition(), is(IndexSettings.btree()));
    assertThat(index.status(), is(BROKEN));

    indexingWithMissing.put(new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), LIVE));

    assertThat(index.status(), is(LIVE));
  }

  @Test
  public void testAllDeadOrMissing() throws Exception {
    Collection<Index<String>> knownIndexes = Arrays.asList(
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), DEAD),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), DEAD),
        new ImmutableIndex<>(stringCellDef, IndexSettings.btree(), DEAD)
    );

    List<DatasetEntity<String>> missing = Collections.singletonList(
        getIndexingDataset("first", new TestIndexing())
    );

    AggregatedIndex<String> index =
        new AggregatedIndex<>(stringCellDef, IndexSettings.btree(), knownIndexes, missing);
    assertThat(index.on(), is(stringCellDef));
    assertThat(index.definition(), is(IndexSettings.btree()));
    assertThat(index.status(), is(DEAD));
  }

  @Test
  public void testAllMissing() throws Exception {
    List<DatasetEntity<String>> missing = Arrays.asList(
        getIndexingDataset("first", new TestIndexing()),
        getIndexingDataset("second", new TestIndexing()),
        getIndexingDataset("third", new TestIndexing()),
        getIndexingDataset("fourth", new TestIndexing())
    );

    AggregatedIndex<String> index =
        new AggregatedIndex<>(stringCellDef, IndexSettings.btree(), Collections.emptyList(), missing);
    assertThat(index.on(), is(stringCellDef));
    assertThat(index.definition(), is(IndexSettings.btree()));
    assertThat(index.status(), is(DEAD));
  }
}