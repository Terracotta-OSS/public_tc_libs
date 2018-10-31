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
package com.terracottatech.store.common;

import com.terracottatech.store.Type;
import org.junit.Test;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.indexing.IndexSettings.btree;
import static java.util.Collections.singletonMap;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DatasetEntityConfigurationTest {
  @Test
  public void gettersReturnConstructionValues() {
    ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration("offheap", "disk",
            singletonMap(defineString("foo"), btree()));
    DatasetEntityConfiguration<String> configuration = new DatasetEntityConfiguration<>(Type.STRING, "name", datasetConfiguration);
    assertEquals(Type.STRING, configuration.getKeyType());
    assertEquals("name", configuration.getDatasetName());
    assertEquals("offheap", configuration.getDatasetConfiguration().getOffheapResource());
    assertEquals("disk", configuration.getDatasetConfiguration().getDiskResource().get());
    assertThat(configuration.getDatasetConfiguration().getIndexes(), hasEntry(defineString("foo"), btree()));
  }
}
