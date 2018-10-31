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
package com.terracottatech.store.server.replication;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexing;
import com.terracottatech.sovereign.impl.persistence.AbstractStorage;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Type;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicationUtilTest {
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void activeUUIDTransformedToPassiveUUID() {
    UUID activeDatasetUUID = UUID.randomUUID();
    UUID passiveDatasetUUID = UUID.randomUUID();

    // Set-up active dataset
    SovereignDatasetImpl<Long> activeDataset = mock(SovereignDatasetImpl.class);
    when(activeDataset.getUUID()).thenReturn(activeDatasetUUID);
    SovereignDataSetConfig config = new SovereignDataSetConfig<>(Type.LONG, SystemTimeReference.class);
    config.alias("MyDataset");
    config.freeze();
    when(activeDataset.getConfig()).thenReturn(config);
    SimpleIndexing<Long> indexing = new SimpleIndexing<>(activeDataset);
    when(activeDataset.getIndexing()).thenReturn(indexing);

    // Set-up passive dataset
    SovereignDatasetImpl passiveDataset = mock(SovereignDatasetImpl.class);
    when(passiveDataset.getUUID()).thenReturn(passiveDatasetUUID);
    AbstractStorage storage = mock(AbstractStorage.class);
    when(passiveDataset.getStorage()).thenReturn(storage);

    // Call the method under test
    SovereignDatasetDescriptionImpl descriptor = new SovereignDatasetDescriptionImpl<Long, SystemTimeReference>(activeDataset);
    ReplicationUtil.setMetadataAndInstallCallbacks(passiveDataset, MetadataKey.Tag.DATASET_DESCR, descriptor);

    // Recover the results
    ArgumentCaptor<MetadataKey> metadataKeyCaptor = ArgumentCaptor.forClass(MetadataKey.class);
    ArgumentCaptor<SovereignDatasetDescriptionImpl> metadataCaptor = ArgumentCaptor.forClass(SovereignDatasetDescriptionImpl.class);
    verify(storage).setMetadata(metadataKeyCaptor.capture(), metadataCaptor.capture());

    // Check that the right MetadataKey was used
    MetadataKey metadataKey = metadataKeyCaptor.getValue();
    assertEquals(MetadataKey.Tag.DATASET_DESCR, metadataKey.getTag());
    assertEquals(passiveDatasetUUID, metadataKey.getUUID());

    // Crucially check that the UUID on the SovereignDatasetDescription matches the passive Dataset UUID
    SovereignDatasetDescriptionImpl<Long, SystemTimeReference> metadata = metadataCaptor.getValue();
    assertEquals("MyDataset", metadata.getAlias());
    assertEquals(passiveDatasetUUID, metadata.getUUID());
  }
}
