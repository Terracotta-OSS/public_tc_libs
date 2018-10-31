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
package com.terracottatech.store.client;

import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import org.junit.Test;

import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.indexing.IndexSettings.btree;
import static java.util.Collections.singletonMap;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ClusteredDatasetConfigurationTest {
  @Test
  public void returnsValuesPassedToConstructor() {
    ClusteredDatasetConfiguration configuration = new ClusteredDatasetConfiguration("A", "B",
            singletonMap(defineString("foo"), btree()));
    assertEquals("A", configuration.getOffheapResource());
    assertEquals("B", configuration.getDiskResource().get());
    assertThat(configuration.getIndexes(), hasEntry(defineString("foo"), btree()));
  }
}
