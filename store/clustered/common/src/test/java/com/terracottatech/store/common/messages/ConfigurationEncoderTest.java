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
package com.terracottatech.store.common.messages;

import org.junit.Test;

import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.configuration.BaseDiskDurability;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageEngine;

import java.util.concurrent.TimeUnit;

import static com.terracottatech.store.configuration.DiskDurability.*;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.indexing.IndexSettings.btree;
import static java.util.Collections.singletonMap;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConfigurationEncoderTest {
  @Test
  public void testBasicConfigurationEncodeDecode() {
    ClusteredDatasetConfiguration datasetConfiguration =
        new ClusteredDatasetConfiguration("offheap", "disk",
        singletonMap(defineString("foo"), btree()));
    DatasetEntityConfiguration<String> configuration = new DatasetEntityConfiguration<>(Type.STRING, "name",
        datasetConfiguration);
    byte[] encodedConfiguration = ConfigurationEncoder.encode(configuration);
    DatasetEntityConfiguration<?> configuration1 = ConfigurationEncoder.decode(encodedConfiguration);
    assertThat(configuration1.getDatasetName(), is("name"));
    assertThat(configuration1.getDatasetConfiguration().getDiskResource().get(), is("disk"));
    assertThat(configuration1.getDatasetConfiguration().getOffheapResource(), is("offheap"));
  }

  @Test
  public void testConfigurationEncodeDecodeWithDiskDurability() {
    ClusteredDatasetConfiguration datasetConfiguration =
        new ClusteredDatasetConfiguration("offheap", "disk",
            singletonMap(defineString("foo"), btree()), null,
            BaseDiskDurability.timed(10L, TimeUnit.SECONDS));
    DatasetEntityConfiguration<String> configuration = new DatasetEntityConfiguration<>(Type.STRING, "name",
        datasetConfiguration);
    byte[] encodedConfiguration = ConfigurationEncoder.encode(configuration);
    DatasetEntityConfiguration<?> configuration1 = ConfigurationEncoder.decode(encodedConfiguration);
    assertThat(configuration1.getDatasetName(), is("name"));
    assertThat(configuration1.getDatasetConfiguration().getDiskResource().get(), is("disk"));
    assertThat(configuration1.getDatasetConfiguration().getOffheapResource(), is("offheap"));
    assertThat(configuration1.getDatasetConfiguration().getDiskDurability().get().getDurabilityEnum(),
        is(DiskDurabilityEnum.TIMED));
    DiskDurability timed = configuration1.getDatasetConfiguration().getDiskDurability().get();
    assertThat(timed, instanceOf(BaseDiskDurability.Timed.class));
    BaseDiskDurability.Timed timed1 = (BaseDiskDurability.Timed) timed;
    assertThat(timed1.getMillisDuration(), is(10000L));
  }

  @Test
  public void testConfigurationEncodeDecodeWithStorageType() {
    ClusteredDatasetConfiguration datasetConfiguration =
        new ClusteredDatasetConfiguration("offheap", "disk",
            singletonMap(defineString("foo"), btree()), null,
            null, PersistentStorageEngine.HYBRID);
    DatasetEntityConfiguration<String> configuration = new DatasetEntityConfiguration<>(Type.STRING, "name",
        datasetConfiguration);
    byte[] encodedConfiguration = ConfigurationEncoder.encode(configuration);
    DatasetEntityConfiguration<?> configuration1 = ConfigurationEncoder.decode(encodedConfiguration);
    assertThat(configuration1.getDatasetName(), is("name"));
    assertThat(configuration1.getDatasetConfiguration().getDiskResource().get(), is("disk"));
    assertThat(configuration1.getDatasetConfiguration().getOffheapResource(), is("offheap"));
    assertThat(configuration1.getDatasetConfiguration().getPersistentStorageType().get(), is(PersistentStorageEngine.HYBRID));
  }
}
