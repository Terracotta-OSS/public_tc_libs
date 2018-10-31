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
package com.terracottatech.store.builder;

import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.indexing.IndexSettings;
import org.junit.Test;

import java.util.Map;

import static com.terracottatech.store.configuration.DiskDurability.DiskDurabilityEnum.EVERY_MUTATION;
import static com.terracottatech.store.indexing.IndexSettings.BTREE;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class EmbeddedDatasetConfigurationBuilderTest {
  @Test(expected = StoreRuntimeException.class)
  public void builderNothing() {
    EmbeddedDatasetConfigurationBuilder builder = new EmbeddedDatasetConfigurationBuilder();
    builder.build();
  }

  @Test
  public void builderOffheap() {
    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) new EmbeddedDatasetConfigurationBuilder()
            .offheap("offheap")
            .build();
    assertEquals("offheap", configuration.getOffheapResource());
    assertFalse(configuration.getDiskResource().isPresent());
    assertEquals(0, configuration.getIndexes().size());
  }

  @Test
  public void builderFull() {
    CellDefinition<?> nameCellDef = CellDefinition.define("name", Type.STRING);

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) new EmbeddedDatasetConfigurationBuilder()
            .offheap("offheap")
            .disk("disk")
            .index(nameCellDef, BTREE)
            .durabilityEveryMutation()
            .build();

    assertEquals("offheap", configuration.getOffheapResource());
    assertEquals("disk", configuration.getDiskResource().orElse(""));
    assertFalse(configuration.getPersistentStorageType().isPresent());
    Map<CellDefinition<?>, IndexSettings> indexes = configuration.getIndexes();
    assertEquals(1, indexes.size());
    assertEquals(BTREE, indexes.get(nameCellDef));
    assertThat(configuration.getDiskDurability().map(DiskDurability::getDurabilityEnum), is(of(EVERY_MUTATION)));
  }

  @Test
  public void builderFullWithStorageType() {
    CellDefinition<?> nameCellDef = CellDefinition.define("name", Type.STRING);

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) new EmbeddedDatasetConfigurationBuilder()
        .offheap("offheap")
        .disk("disk", PersistentStorageEngine.HYBRID)
        .index(nameCellDef, BTREE)
        .durabilityEveryMutation()
        .build();

    assertEquals("offheap", configuration.getOffheapResource());
    assertEquals("disk", configuration.getDiskResource().orElse(""));
    Map<CellDefinition<?>, IndexSettings> indexes = configuration.getIndexes();
    assertEquals(1, indexes.size());
    assertEquals(BTREE, indexes.get(nameCellDef));
    assertThat(configuration.getDiskDurability().map(DiskDurability::getDurabilityEnum), is(of(EVERY_MUTATION)));
  }

  @Test
  public void useBuilderAsPrototype() {
    CellDefinition<?> nameCellDef = CellDefinition.define("name", Type.STRING);
    CellDefinition<?> addressCellDef = CellDefinition.define("address", Type.STRING);

    DatasetConfigurationBuilder builder1 = new EmbeddedDatasetConfigurationBuilder()
            .offheap("offheap1")
            .disk("disk1")
            .index(nameCellDef, BTREE);

    DatasetConfigurationBuilder builder2 = builder1.offheap("offheap2")
            .disk("disk2")
            .index(addressCellDef, BTREE);

    EmbeddedDatasetConfiguration configuration1 = (EmbeddedDatasetConfiguration) builder1.build();
    EmbeddedDatasetConfiguration configuration2 = (EmbeddedDatasetConfiguration) builder2.build();

    assertEquals("offheap1", configuration1.getOffheapResource());
    assertEquals("disk1", configuration1.getDiskResource().orElse(""));
    Map<CellDefinition<?>, IndexSettings> indexes1 = configuration1.getIndexes();
    assertEquals(1, indexes1.size());
    assertEquals(BTREE, indexes1.get(nameCellDef));

    assertEquals("offheap2", configuration2.getOffheapResource());
    assertEquals("disk2", configuration2.getDiskResource().orElse(""));
    Map<CellDefinition<?>, IndexSettings> indexes2 = configuration2.getIndexes();
    assertEquals(2, indexes2.size());
    assertEquals(BTREE, indexes2.get(nameCellDef));
    assertEquals(BTREE, indexes2.get(addressCellDef));
  }
}
