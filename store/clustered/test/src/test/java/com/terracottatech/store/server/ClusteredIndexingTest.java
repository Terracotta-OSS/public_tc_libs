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

package com.terracottatech.store.server;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.test.data.Animals;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.terracottatech.store.indexing.Index.Status.LIVE;
import static com.terracottatech.store.indexing.IndexSettings.btree;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class ClusteredIndexingTest extends PassthroughTest {

  private final List<String> stripeNames;

  public ClusteredIndexingTest(List<String> stripeNames) {
    this.stripeNames = stripeNames;
  }

  @Override
  protected List<String> provideStripeNames() {
    return stripeNames;
  }

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {Collections.singletonList("stripe"), Arrays.asList("stripe1", "stripe2") };
  }

  @Test
  public void testCreateIndexClosed() throws Exception {
    Indexing indexing = dataset.getIndexing();
    dataset.close();
    expectedException.expect(StoreRuntimeException.class);
    indexing.createIndex(OBSERVATIONS, btree());
  }

  @Test
  public void testCreatingIndexesOnConstruction() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    Animals.recordStream().forEach(r -> writerReader.add(r.getKey(), r));

    List<Index<?>> indexes = dataset.getIndexing().getLiveIndexes().stream()
        .sorted(Comparator.comparing(i -> i.on().name())).collect(toList());

    assertThat(indexes.get(0).on(), is(OBSERVATIONS));
    assertThat(indexes.get(0).definition(), is(btree()));
    assertThat(indexes.get(0).status(), is(LIVE));

    assertThat(indexes.get(1).on(), is(STATUS));
    assertThat(indexes.get(1).definition(), is(btree()));
    assertThat(indexes.get(1).status(), is(LIVE));

    assertThat(writerReader.records().filter(STATUS.value().is(Animals.EXTINCT))
        .map(Record::getKey).collect(toList()), containsInAnyOrder("dimetrodon", "quagga"));
  }

  @Test
  public void testCreateDynamicIndex() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    Animals.recordStream().forEach(r -> writerReader.add(r.getKey(), r));

    Indexing indexing = dataset.getIndexing();
    assertFalse(indexing.getAllIndexes().stream().anyMatch(i -> i.on().equals(TAXONOMIC_CLASS)));

    Operation<Index<String>> taxClassCreateOp = indexing.createIndex(TAXONOMIC_CLASS, btree());

    Index<String> taxClassIndex = taxClassCreateOp.get();
//        assertThat(taxClassIndex, is(instanceOf(LiveIndex.class)));
    assertThat(taxClassIndex, is(notNullValue()));
    assertThat(taxClassIndex.on(), is(TAXONOMIC_CLASS));
    assertThat(taxClassIndex.definition(), is(btree()));
    assertThat(taxClassIndex.status(), is(LIVE));

    Map<? extends CellDefinition<?>, Index<?>> indexMap = indexing.getLiveIndexes()
        .stream()
        .collect(toMap(Index::on, identity()));
    //noinspection RedundantTypeArguments
    assertThat(indexMap, allOf(Matchers.<CellDefinition<?>>hasKey(OBSERVATIONS), hasKey(STATUS), hasKey(TAXONOMIC_CLASS)));
    indexMap.values().forEach(i -> {
          assertThat(i.status(), is(LIVE));
          assertThat(i.definition(), is(btree()));
        }
    );
  }

  @Test
  public void testDestroyIndexClosed() throws Exception {
    Indexing indexing = dataset.getIndexing();
    dataset.close();
    expectedException.expect(StoreRuntimeException.class);
    indexing.destroyIndex(new ImmutableIndex<>(OBSERVATIONS, btree(), LIVE));
  }

  @Test
  public void testDestroyIndex() throws Exception {
    DatasetWriterReader<String> writerReader = dataset.writerReader();
    Animals.recordStream().forEach(r -> writerReader.add(r.getKey(), r));

    Indexing indexing = dataset.getIndexing();
    Operation<Index<String>> taxClassCreateOp = indexing.createIndex(TAXONOMIC_CLASS, btree());
    Index<String> taxClassIndex = taxClassCreateOp.get();

    @SuppressWarnings({"varargs", "unchecked"})
    Matcher<Iterable<? extends CellDefinition<?>>> containsInAnyOrder = containsInAnyOrder(
            OBSERVATIONS, STATUS, TAXONOMIC_CLASS);
    assertThat(indexing.getAllIndexes().stream().map(Index::on).collect(toList()), containsInAnyOrder);

    indexing.destroyIndex(taxClassIndex);

    Map<? extends CellDefinition<?>, Index<?>> indexMap = indexing.getAllIndexes()
        .stream()
        .collect(toMap(Index::on, identity()));
    assertThat(indexMap.keySet(), containsInAnyOrder(OBSERVATIONS, STATUS));

    for (Index<?> index : indexMap.values()) {
      indexing.destroyIndex(index);
    }

    assertThat(indexing.getAllIndexes().stream().map(Index::on).collect(toList()), is(empty()));
  }

  @Test
  public void testGetAllIndexesClosed() throws Exception {
    Indexing indexing = dataset.getIndexing();
    dataset.close();
    expectedException.expect(StoreRuntimeException.class);
    indexing.getAllIndexes();
  }

  @Test
  public void testGetLiveIndexesClosed() throws Exception {
    Indexing indexing = dataset.getIndexing();
    dataset.close();
    expectedException.expect(StoreRuntimeException.class);
    indexing.getLiveIndexes();
  }

  protected Dataset<String> getTestDataset(DatasetManager datasetManager) throws StoreException {
    DatasetConfigurationBuilder configurationBuilder = datasetManager.datasetConfiguration()
        .offheap("offheap")
        .index(OBSERVATIONS, btree())
        .index(STATUS, btree());
    configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
    DatasetConfiguration testConfiguration = configurationBuilder.build();

    datasetManager.newDataset("animals", Type.STRING, testConfiguration);
    return datasetManager.getDataset("animals", Type.STRING);
  }
}
