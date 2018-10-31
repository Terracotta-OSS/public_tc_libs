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
package com.terracottatech.store.server.management;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.indexing.SimpleIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import org.junit.Test;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.service.monitoring.EntityMonitoringService;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatasetSettingsManagementProviderTest {

  @Test
  public void getDescriptorsTest() throws Exception {
    DatasetSettingsManagementProvider datasetSettingsManagementProvider = new DatasetSettingsManagementProvider();
    datasetSettingsManagementProvider.setMonitoringService(mock(EntityMonitoringService.class));

    @SuppressWarnings("unchecked")
    DatasetEntityConfiguration<String> entityConfiguration = mock(DatasetEntityConfiguration.class);

    ClusteredDatasetConfiguration clusteredDatasetConfiguration =  mock(ClusteredDatasetConfiguration.class);
    when(entityConfiguration.getDatasetName()).thenReturn("my-dataset");
    when(entityConfiguration.getKeyType()).thenReturn(Type.STRING);

    when(clusteredDatasetConfiguration.getDiskResource()).thenReturn(Optional.of("disk-resource"));
    when(clusteredDatasetConfiguration.getOffheapResource()).thenReturn("offheap-resource");
    when(clusteredDatasetConfiguration.getConcurrencyHint()).thenReturn(Optional.empty());
    Map<CellDefinition<?>, IndexSettings> indexes =  new HashMap<>();
    indexes.put(CellDefinition.defineBool("bool"), IndexSettings.BTREE);
    indexes.put(CellDefinition.defineString("bill"), IndexSettings.BTREE);
    when(clusteredDatasetConfiguration.getIndexes()).thenReturn(indexes);


    SovereignDataset<?> dataset = mock(SovereignDataset.class);
    SovereignIndexing indexing = mock(SovereignIndexing.class);
    when(dataset.getIndexing()).thenReturn(indexing);
    when(indexing.getIndexes()).thenReturn(Arrays.asList(
            new SimpleIndex<>(CellDefinition.defineBool("bool"), SovereignIndexSettings.BTREE),
            new SimpleIndex<>(CellDefinition.defineString("bill"), SovereignIndexSettings.BTREE)
    ));

    when(entityConfiguration.getDatasetConfiguration()).thenReturn(clusteredDatasetConfiguration);
    datasetSettingsManagementProvider.register(new DatasetBinding(dataset, null, entityConfiguration));
    Collection<? extends Descriptor> descriptors = datasetSettingsManagementProvider.getDescriptors();
    assertThat(descriptors.size(), equalTo(1));
    Settings actual = (Settings) descriptors.iterator().next();
    assertThat(actual.get("alias"), equalTo("my-dataset"));
    assertThat(actual.get("type"), equalTo("SovereignDataset"));
    assertThat(actual.get("keyType"), equalTo("STRING"));
    assertThat(actual.get("datasetName"), equalTo("my-dataset"));
    assertThat(actual.get("offheapResourceName"), equalTo("offheap-resource"));
    assertThat(actual.get("restartableStoreId"), equalTo("disk-resource#store#data"));
    assertThat(actual.get("restartableStoreRoot"), equalTo("disk-resource"));
    assertThat(actual.get("restartableStoreContainer"), equalTo("store"));
    assertThat(actual.get("restartableStoreName"), equalTo("data"));
    assertThat(actual.get("concurrencyHint"), equalTo(-1));
    assertThat(Arrays.asList((String[])actual.get("runtimeIndexes")), hasItems("bool$$$Boolean$$$BTREE", "bill$$$String$$$BTREE"));

  }
}
