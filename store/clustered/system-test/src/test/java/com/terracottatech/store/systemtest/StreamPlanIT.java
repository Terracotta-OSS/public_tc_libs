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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.client.stream.sharded.MultiStripeExplanation;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.testing.rules.EnterpriseCluster;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StreamPlanIT extends BaseSystemTest {
  @ClassRule
  public static EnterpriseCluster CLUSTER = initMultiStripeCluster("OffheapAndFRS.xmlfrag");

  @BeforeClass
  public static void beforeClass() throws Exception {
    CLUSTER.getStripeControl(0).waitForActive();
    CLUSTER.getStripeControl(1).waitForActive();
  }

  @Test
  public void explanationIncludesServerNames() throws Exception {
    try (DatasetManager datasetManager = DatasetManager.clustered(CLUSTER.getConnectionURI()).build()) {
      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration()
              .offheap("primary-server-resource")
              .build();

      assertTrue(datasetManager.newDataset("testing", Type.INT, datasetConfiguration));

      try (Dataset<Integer> dataset = datasetManager.getDataset("testing", Type.INT)) {
        DatasetReader<Integer> reader = dataset.reader();

        AtomicReference<Object> explanation = new AtomicReference<>();
        long count = reader.records().explain(explanation::set).count();
        assertEquals(0, count);

        Set<String> serverNames = ((MultiStripeExplanation) explanation.get())
                .serverPlans()
                .values()
                .stream()
                .map(s -> s.split("\\R"))
                .map(lines -> Stream.of(lines).findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(s -> s.substring("Server Name: ".length()))
                .collect(Collectors.toSet());

        assertThat(serverNames, containsInAnyOrder("testServer0", "testServer100"));
      }
    }
  }
}
