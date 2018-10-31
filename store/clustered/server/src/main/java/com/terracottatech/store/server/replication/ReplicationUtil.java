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

import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.PersistableSchemaList;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.impl.utils.CachingSequence;

public class ReplicationUtil {

  private ReplicationUtil() {
  }

  public static void setMetadataAndInstallCallbacks(SovereignDatasetImpl<?> dataset, MetadataKey.Tag<?> tag, Object value) {
    if (tag.equals(MetadataKey.Tag.DATASET_DESCR)) {
      // The active and the passive have different UUIDs for the dataset, so we need to change the UUID
      // in the SovereignDatasetDescription to match the passive's UUID. See TDB-2576.
      SovereignDatasetDescriptionImpl<?, ?> description = MetadataKey.Tag.DATASET_DESCR.cast(value);
      value = SovereignDatasetDescriptionImpl.normalizeUUID(description, dataset.getUUID());
    }

    dataset.getStorage().setMetadata(tag.keyFor(dataset.getUUID()), value);

    if (tag.equals(MetadataKey.Tag.SCHEMA)) {
      dataset.getRuntime().getSchema().reload((PersistableSchemaList) value);
      dataset.getRuntime().getSchema().getBackend().setCallback(x ->
              dataset.getStorage()
                      .setMetadata(MetadataKey.Tag.SCHEMA.keyFor(dataset.getUUID()),
                              dataset.getRuntime().getSchema().getBackend().getPersistable()));
    }

    if (tag.equals(MetadataKey.Tag.CACHING_SEQUENCE)) {
      dataset.getRuntime().setSequence((CachingSequence) value);
      dataset.getRuntime().getSequence().setCallback(x ->
              dataset.getStorage().setMetadata(MetadataKey.Tag.CACHING_SEQUENCE.keyFor(dataset.getUUID()),
                      dataset.getRuntime().getSequence()));
    }
  }
}
