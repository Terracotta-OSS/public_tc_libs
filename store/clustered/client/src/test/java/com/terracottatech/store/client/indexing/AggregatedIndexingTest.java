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

import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.CharCellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.terracottatech.store.client.indexing.DatasetEntityProxies.getIndexingDataset;
import static com.terracottatech.store.indexing.Index.Status.BROKEN;
import static com.terracottatech.store.indexing.Index.Status.DEAD;
import static com.terracottatech.store.indexing.Index.Status.INITALIZING;
import static com.terracottatech.store.indexing.Index.Status.LIVE;
import static com.terracottatech.store.indexing.Index.Status.POTENTIAL;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Basic tests for {@link AggregatedIndexing}.
 */
public class AggregatedIndexingTest {

  private static final IndexSettings BTREE = IndexSettings.btree();

  private final StringCellDefinition preDefinedStringDef = CellDefinition.defineString("preDefinedString");
  private final StringCellDefinition nonDefinedStringDef = CellDefinition.defineString("nonDefinedString");
  private final StringCellDefinition partialFaultStringDef = CellDefinition.defineString("partialFaultString");
  private final StringCellDefinition fullImmediateFaultStringDef = CellDefinition.defineString("fullImmediateFaultString");

  @Test
  public void testCreateIndexClosed() throws Exception {
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(Collections.emptyList(), () -> true);
    try {
      indexing.createIndex(nonDefinedStringDef, BTREE);
      fail("Expecting StoreRuntimeException");
    } catch (StoreRuntimeException e) {
      // expected
    }
  }

  @Test
  public void testCreateIndexComplete() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first", new TestIndexing()),
        getIndexingDataset("second", new TestIndexing()),
        getIndexingDataset("third", new TestIndexing()),
        getIndexingDataset("fourth", new TestIndexing())
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Operation<Index<String>> indexOp = indexing.createIndex(nonDefinedStringDef, BTREE);
    Index<String> index = indexOp.get();
    assertThat(index.on(), is(nonDefinedStringDef));
    assertThat(index.definition(), is(BTREE));
    assertThat(index.status(), is(LIVE));
    assertTrue(allHaveIndex(entityList, index));
  }

  @Test
  public void testCreateIndexExisting() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("second",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("third",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("fourth",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE)))
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Operation<Index<String>> indexOp = indexing.createIndex(preDefinedStringDef, BTREE);
    try {
      indexOp.get();
      fail("Expecting ExecutionException");
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(instanceOf(StoreRuntimeException.class)));
    }
    assertTrue(allHaveIndex(entityList, new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE)));
  }

  @Test
  public void testCreateIndexQuickFail() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first",
            new TestIndexing().put(fullImmediateFaultStringDef, BTREE, new TestIndexing.ImmediateFailure())),
        getIndexingDataset("second",
            new TestIndexing().put(fullImmediateFaultStringDef, BTREE, new TestIndexing.ImmediateFailure())),
        getIndexingDataset("third",
            new TestIndexing().put(fullImmediateFaultStringDef, BTREE, new TestIndexing.ImmediateFailure())),
        getIndexingDataset("fourth",
            new TestIndexing().put(fullImmediateFaultStringDef, BTREE, new TestIndexing.ImmediateFailure()))
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    try {
      indexing.createIndex(fullImmediateFaultStringDef, BTREE);
      fail("Expecting StoreRuntimeException");
    } catch (StoreRuntimeException e) {
      assertThat(e.getSuppressed(), is(arrayWithSize(entityList.size())));
    }
    assertFalse(anyHaveIndex(entityList, new ImmutableIndex<>(fullImmediateFaultStringDef, BTREE, LIVE)));
  }

  @Test
  public void testCreateIndexPartialFault() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first", new TestIndexing()),
        getIndexingDataset("second",
            new TestIndexing().put(partialFaultStringDef, BTREE, new RuntimeException("bad"))),
        getIndexingDataset("third",
            new TestIndexing().put(partialFaultStringDef, BTREE, new TestIndexing.ImmediateFailure())),
        getIndexingDataset("fourth", new TestIndexing())
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Operation<Index<String>> indexOp = indexing.createIndex(partialFaultStringDef, BTREE);
    Index<String> index = indexOp.get();
    assertThat(index.on(), is(partialFaultStringDef));
    assertThat(index.status(), is(BROKEN));
    assertFalse(allHaveIndex(entityList, index));
    assertTrue(anyHaveIndex(entityList, index));
  }

  @Test
  public void testCreateIndexFillIn() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first",
            new TestIndexing()
                .put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("second", new TestIndexing()),
        getIndexingDataset("third", new TestIndexing()),
        getIndexingDataset("fourth",
            new TestIndexing()
                .put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE)))
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Operation<Index<String>> indexOp = indexing.createIndex(partialFaultStringDef, BTREE);
    Index<String> index = indexOp.get();
    assertThat(index.on(), is(partialFaultStringDef));
    assertThat(index.status(), is(LIVE));
    assertTrue(allHaveIndex(entityList, index));
  }

  @Test
  public void testDestroyIndexClosed() throws Exception {
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(Collections.emptyList(), () -> true);
    try {
      indexing.destroyIndex(new ImmutableIndex<>(nonDefinedStringDef, BTREE, LIVE));
      fail("Expecting StoreRuntimeException");
    } catch (StoreRuntimeException e) {
      // expected
    }
  }

  @Test
  public void testDestroyIndexAllDefined() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("second",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("third",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("fourth",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE)))
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Index<String> index = new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE);
    indexing.destroyIndex(index);
    assertFalse(anyHaveIndex(entityList, index));
  }

  @Test
  public void testDestroyIndexNoneDefined() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first", new TestIndexing()),
        getIndexingDataset("second", new TestIndexing()),
        getIndexingDataset("third", new TestIndexing()),
        getIndexingDataset("fourth", new TestIndexing())
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Index<String> index = new ImmutableIndex<>(nonDefinedStringDef, BTREE, LIVE);
    try {
      indexing.destroyIndex(index);
      fail("Expecting StoreIndexNotFoundException");
    } catch (StoreIndexNotFoundException e) {
      assertThat(e.getSuppressed(), is(arrayWithSize(entityList.size())));
    }
    assertFalse(anyHaveIndex(entityList, index));
  }

  @Test
  public void testDestroyIndexSomeDefined() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("second", new TestIndexing()),
        getIndexingDataset("third",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("fourth", new TestIndexing())
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Index<String> index = new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE);
    indexing.destroyIndex(index);
    assertFalse(anyHaveIndex(entityList, index));
  }

  @Test
  public void testDestroyIndexSomeFailed() throws Exception {
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("second", new TestIndexing()),
        getIndexingDataset("third",
            new TestIndexing().put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))),
        getIndexingDataset("fourth", new TestIndexing()
            .put(preDefinedStringDef, BTREE, new TestIndexing.ImmediateFailure()))
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    Index<String> index = new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE);
    try {
      indexing.destroyIndex(index);
      fail("Expecting ImmediateFailure");
    } catch (TestIndexing.ImmediateFailure e) {
      e.printStackTrace();
      assertThat(e.getSuppressed(), is(arrayWithSize(1)));
    }
    assertFalse(anyHaveIndex(entityList, index));
  }

  @Test
  public void testGetAllIndexesClosed() throws Exception {
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(Collections.emptyList(), () -> true);
    try {
      indexing.getAllIndexes();
      fail("Expecting StoreRuntimeException");
    } catch (StoreRuntimeException e) {
      // expected
    }
  }

  @Test
  public void testGetLiveIndexesClosed() throws Exception {
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(Collections.emptyList(), () -> true);
    try {
      indexing.getLiveIndexes();
      fail("Expecting StoreRuntimeException");
    } catch (StoreRuntimeException e) {
      // expected
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetIndexes() throws Exception {
    IntCellDefinition someInitializing = CellDefinition.defineInt("someInitializing");
    LongCellDefinition somePotential = CellDefinition.defineLong("somePotential");
    DoubleCellDefinition someDead = CellDefinition.defineDouble("someDead");
    CharCellDefinition someMissing = CellDefinition.defineChar("someMissing");
    BoolCellDefinition allDead = CellDefinition.defineBool("allDead");
    StringCellDefinition mostlyDead = CellDefinition.defineString("mostlyDead");
    List<DatasetEntity<String>> entityList = Arrays.asList(
        getIndexingDataset("first",
            new TestIndexing()
                .put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))
                .put(new ImmutableIndex<>(somePotential, BTREE, LIVE))
                .put(new ImmutableIndex<>(someInitializing, BTREE, INITALIZING))
                .put(new ImmutableIndex<>(someDead, BTREE, LIVE))
                .put(new ImmutableIndex<>(someMissing, BTREE, LIVE))
                .put(new ImmutableIndex<>(allDead, BTREE, DEAD))
                .put(new ImmutableIndex<>(mostlyDead, BTREE, DEAD))),
        getIndexingDataset("second",
            new TestIndexing()
                .put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))
                .put(new ImmutableIndex<>(somePotential, BTREE, LIVE))
                .put(new ImmutableIndex<>(someInitializing, BTREE, LIVE))
                .put(new ImmutableIndex<>(someDead, BTREE, LIVE))
                .put(new ImmutableIndex<>(allDead, BTREE, DEAD))),
        getIndexingDataset("third",
            new TestIndexing()
                .put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))
                .put(new ImmutableIndex<>(somePotential, BTREE, POTENTIAL))
                .put(new ImmutableIndex<>(someInitializing, BTREE, LIVE))
                .put(new ImmutableIndex<>(someDead, BTREE, DEAD))
                .put(new ImmutableIndex<>(someMissing, BTREE, LIVE))
                .put(new ImmutableIndex<>(allDead, BTREE, DEAD))
                .put(new ImmutableIndex<>(mostlyDead, BTREE, DEAD))),
        getIndexingDataset("fourth",
            new TestIndexing()
                .put(new ImmutableIndex<>(preDefinedStringDef, BTREE, LIVE))
                .put(new ImmutableIndex<>(somePotential, BTREE, LIVE))
                .put(new ImmutableIndex<>(someInitializing, BTREE, INITALIZING))
                .put(new ImmutableIndex<>(someDead, BTREE, DEAD))
                .put(new ImmutableIndex<>(someMissing, BTREE, LIVE))
                .put(new ImmutableIndex<>(allDead, BTREE, DEAD))
                .put(new ImmutableIndex<>(mostlyDead, BTREE, DEAD)))
    );
    AggregatedIndexing<String> indexing = new AggregatedIndexing<>(entityList, () -> false);

    CellDefinition<?>[] indexDefinitions = entityList.stream()
        .flatMap(e -> e.getIndexing().getAllIndexes().stream())
        .map(Index::on)
        .distinct()
        .toArray(CellDefinition<?>[]::new);

    Collection<Index<?>> allIndexes = indexing.getAllIndexes();
    assertThat(allIndexes.stream().map(Index::on).collect(toList()), containsInAnyOrder(indexDefinitions));
    assertThat(getIndex(allIndexes, preDefinedStringDef, BTREE).status(), is(LIVE));
    assertThat(getIndex(allIndexes, someInitializing, BTREE).status(), is(INITALIZING));
    assertThat(getIndex(allIndexes, somePotential, BTREE).status(), is(POTENTIAL));
    assertThat(getIndex(allIndexes, someDead, BTREE).status(), is(DEAD));
    assertThat(getIndex(allIndexes, someMissing, BTREE).status(), is(BROKEN));
    assertThat(getIndex(allIndexes, allDead, BTREE).status(), is(DEAD));
    assertThat(getIndex(allIndexes, mostlyDead, BTREE).status(), is(DEAD));

    Collection<Index<?>> liveIndexes = indexing.getLiveIndexes();
    assertThat(liveIndexes.stream().map(Index::on).collect(toList()), contains(preDefinedStringDef));
  }

  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> Index<T> getIndex(Collection<Index<?>> indexes, CellDefinition<T> on, IndexSettings settings) {
    return indexes.stream()
        .filter(i -> i.on().equals(on) && i.definition().equals(settings))
        .findAny().map(i -> (Index<T>)i).orElseThrow(AssertionError::new);
  }

  private boolean anyHaveIndex(List<DatasetEntity<String>> entityList, Index<?> index) {
    boolean anyHaveIndex = false;
    for (DatasetEntity<String> entity : entityList) {
      anyHaveIndex |= entity.getIndexing().getAllIndexes().stream()
          .anyMatch(i -> i.on().equals(index.on()) && i.definition().equals(index.definition()));
    }
    return anyHaveIndex;
  }

  private boolean allHaveIndex(List<DatasetEntity<String>> entityList, Index<?> index) {
    boolean allHaveIndex = true;
    for (DatasetEntity<String> entity : entityList) {
      allHaveIndex &= entity.getIndexing().getAllIndexes().stream()
          .anyMatch(i -> i.on().equals(index.on()) && i.definition().equals(index.definition()));
    }
    return allHaveIndex;
  }
}