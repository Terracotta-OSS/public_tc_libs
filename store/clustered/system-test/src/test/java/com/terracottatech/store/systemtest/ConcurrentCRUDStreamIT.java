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
package com.terracottatech.store.systemtest;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.tc.properties.TCPropertiesConsts;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.manager.ClusteredDatasetManagerBuilder;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.testing.rules.EnterpriseCluster;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import static com.terracottatech.store.UpdateOperation.write;
import static com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder.newCluster;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Performs tests using concurrent streams and CRUD operations.
 */
@SuppressWarnings("Duplicates")
public class ConcurrentCRUDStreamIT {

  private static final String CLUSTER_OFFHEAP_RESOURCE = "primary-server-resource";
  private static final String PERSISTENCE_RESOURCE = "root";
  private static final String SERVICE_CONFIG =
      "<config xmlns:ohr=\"http://www.terracotta.org/config/offheap-resource\">\n" +
      "  <ohr:offheap-resources>\n" +
      "    <ohr:resource name=\"" + CLUSTER_OFFHEAP_RESOURCE + "\" unit=\"MB\">64</ohr:resource>\n" +
      "  </ohr:offheap-resources>\n" +
      "</config>\n" +
      "<config xmlns:data=\"http://www.terracottatech.com/config/data-roots\">\n" +
      "  <data:data-directories>\n" +
      "    <data:directory name=\"" + PERSISTENCE_RESOURCE + "\" use-for-platform=\"true\">data</data:directory>\n" +
      "  </data:data-directories>\n" +
      "</config>\n";

  /**
   * Establishes the value for {@link com.tc.properties.TCPropertiesConsts#ENTITY_PROCESSOR_THREADS}
   * provided to the server(s).  This sets the number of threads created for the server to process
   * incoming messages.  (The default value is calculated; see
   * {@code com.tc.net.utils.L2Utils#getOptimalApplyStageWorkerThreads(boolean)}.)
   */
  private static final int ENTITY_PROCESSOR_THREADS = 4;

  private static final Properties TC_PROPERTIES;
  static {
    Properties tcProperties = new Properties();
    tcProperties.setProperty(TCPropertiesConsts.ENTITY_PROCESSOR_THREADS, Integer.toString(ENTITY_PROCESSOR_THREADS));
    TC_PROPERTIES = tcProperties;
  }

  private static final Properties SERVER_PROPERTIES;
  static {
    Properties serverProperties = new Properties();
    serverProperties.setProperty("com.terracottatech.store.mutative-pipelines.enable-all", "true");
    SERVER_PROPERTIES = serverProperties;
  }

  @Rule
  public EnterpriseCluster CLUSTER = newCluster(1)
      .withPlugins(SERVICE_CONFIG)
      .withTcProperties(TC_PROPERTIES)
      .withServerProperties(SERVER_PROPERTIES)
      .build();

  private DatasetManager datasetManager;
  private DatasetConfiguration datasetConfiguration;

  @Before
  @SuppressWarnings("deprecation")
  public void setUp() throws Exception {
    CLUSTER.getClusterControl().waitForActive();

    ClusteredDatasetManagerBuilder datasetManagerBuilder = DatasetManager.clustered(CLUSTER.getConnectionURI());
    datasetManager = datasetManagerBuilder.build();
    DatasetConfigurationBuilder configurationBuilder =
        datasetManager.datasetConfiguration().offheap(CLUSTER_OFFHEAP_RESOURCE);
    configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
    datasetConfiguration = configurationBuilder.build();
  }

  @After
  public void tearDown() throws Exception {
    /*
     * Don't bother attempting to destroy the dataset(s) -- if the hang occurs, it's due to
     * Voltron message processing thread "exhaustion" and there'd be no more threads to
     * process the destroy messages.
     */
    datasetManager.close();
  }

  @Test
  public void testConcurrentUpdatingNonPortableStream() throws Exception {
    int parallelism = ENTITY_PROCESSOR_THREADS * 2;
    long recordCount = 256L;
    long testTimeoutMinutes = 2;
    System.out.format("EntityProcessorThreads=%d, parallelism=%d, recordCount=%d, timeout=%d minutes%n",
        ENTITY_PROCESSOR_THREADS, parallelism, recordCount, testTimeoutMinutes);

    datasetManager.newDataset("dataset", Type.LONG, datasetConfiguration);
    Dataset<Long> dataset = datasetManager.getDataset("dataset", Type.LONG);

    DatasetWriterReader<Long> writerReader = dataset.writerReader();

    LongCellDefinition longCellDefinition = CellDefinition.defineLong("long");

    final ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

    for (long i = 0L; i < recordCount; i++) {
      writerReader.add(i, longCellDefinition.newCell(1L));
    }

    ArrayList<MutableRecordStream<Long>> streams = new ArrayList<>();
    for (int i = 0; i < parallelism / 2; i++) {
      streams.add(writerReader.records());
    }

    ToLongFunction<Record<?>> transform = (record) -> record.get(longCellDefinition).orElse(0L) + 1;
    for (MutableRecordStream<Long> stream : streams) {
      executorService.execute(() -> stream.mutate(write(longCellDefinition).longResultOf(transform)));
      executorService.execute(() -> {
        for (long j = 0L; j < recordCount; j++) {
          long key = j;
          writerReader.get(key).ifPresent(r -> writerReader.on(key).update(write(longCellDefinition).longResultOf(transform)));
        }
      });
    }
    executorService.shutdown();
    assertTrue("Timed out: processing not complete within " + testTimeoutMinutes + " minutes",
        executorService.awaitTermination(testTimeoutMinutes, TimeUnit.MINUTES));

    ArrayList<Long> results = new ArrayList<>();
    for (long i = 0L; i < recordCount; i++) {
      results.add(writerReader.get(i).get().get(longCellDefinition).get());
    }

    for (int i = 0; i < results.size(); i++) {
      Long l = results.get(i);
      if (l != parallelism + 1L) {
        System.err.format("key[%d]=%d%n", i, l);
      }
    }
    assertThat(results, everyItem(is(parallelism + 1L)));
  }

  @Test
  public void testConcurrentNonPortableCrudUpdate() throws Exception {
    int parallelism = ENTITY_PROCESSOR_THREADS * 2;
    long repetitionCount = 256L;
    long testTimeoutMinutes = ENTITY_PROCESSOR_THREADS / 8 + 1;
    System.out.format("EntityProcessorThreads=%d, parallelism=%d, repetitionCount=%d, timeout=%d minutes%n",
        ENTITY_PROCESSOR_THREADS, parallelism, repetitionCount, testTimeoutMinutes);

    datasetManager.newDataset("dataset", Type.LONG, datasetConfiguration);
    Dataset<Long> dataset = datasetManager.getDataset("dataset", Type.LONG);

    DatasetWriterReader<Long> writerReader = dataset.writerReader();

    LongCellDefinition longCellDefinition = CellDefinition.defineLong("long");

    final ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

    /*
     * Write the single record all concurrent operations are going to hit.
     */
    writerReader.add(1L, longCellDefinition.newCell(0L));

    ToLongFunction<Record<?>> transform = (record) -> record.get(longCellDefinition).orElse(0L) + 1;
    for (int i = 0; i < parallelism; i++) {
      executorService.execute(() -> {
        for (int j = 0; j < repetitionCount; j++) {
          writerReader.get(1L).ifPresent(r -> writerReader.on(1L).update(write(longCellDefinition).longResultOf(transform)));
        }
      });
    }
    executorService.shutdown();
    assertTrue("Timed out: processing not complete within " + testTimeoutMinutes + " minutes",
        executorService.awaitTermination(testTimeoutMinutes, TimeUnit.MINUTES));

    long result = writerReader.get(1L).get().get(longCellDefinition).get();
    assertThat(result, is(parallelism * repetitionCount));
  }
}
