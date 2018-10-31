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
package com.terracottatech.store.server.messages.replication;

import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.PersistableSchemaList;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.server.messages.CachingSequenceEncoder;
import com.terracottatech.store.server.messages.PersistableSchemaListEncoder;
import com.terracottatech.store.server.messages.ReplicationMessageType;
import com.terracottatech.store.server.messages.SovereignDatasetDescriptionEncoder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;

import static com.terracottatech.store.server.messages.CachingSequenceEncoder.CACHING_SEQUENCE_STRUCT;
import static com.terracottatech.store.server.messages.PersistableSchemaListEncoder.PERSISTABLE_SCHEMA_LIST_STRUCT;
import static com.terracottatech.store.server.messages.SovereignDatasetDescriptionEncoder.SOVEREIGN_DATASET_DESCRIPTION_STRUCT;

public class MetadataReplicationMessage extends ReplicationMessage {

  private static final String INDEX = "index";

  private int index;
  private Object metadata;

  public MetadataReplicationMessage(int index, Object metadata) {
    this.index = index;
    this.metadata = metadata;
  }

  public int getIndex() {
    return this.index;
  }

  public Object getMetadata() {
    return this.metadata;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.int32(INDEX, 170)
        .struct("persistableSchemaList", 180, PERSISTABLE_SCHEMA_LIST_STRUCT)
        .struct("cachingSequence", 190, CACHING_SEQUENCE_STRUCT)
        .struct("sovereignDatasetDescription", 200, SOVEREIGN_DATASET_DESCRIPTION_STRUCT)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, ReplicationMessage message) {
    MetadataReplicationMessage metadataReplicationMessage = (MetadataReplicationMessage) message;
    encoder.int32(INDEX, metadataReplicationMessage.getIndex());

    Object metadata = metadataReplicationMessage.getMetadata();

    if (metadata instanceof PersistableSchemaList) {
      PersistableSchemaList persistableSchemaList = (PersistableSchemaList) metadata;
      encoder.getUnderlying().struct("persistableSchemaList", persistableSchemaList, PersistableSchemaListEncoder::encode);
    } else if (metadata instanceof CachingSequence) {
      CachingSequence cachingSequence = (CachingSequence) metadata;
      encoder.getUnderlying().struct("cachingSequence", cachingSequence, CachingSequenceEncoder::encode);
    } else if (metadata instanceof SovereignDatasetDescriptionImpl) {
      SovereignDatasetDescriptionImpl<?, ?> sovereignDatasetDescription = (SovereignDatasetDescriptionImpl) metadata;
      encoder.getUnderlying().struct("sovereignDatasetDescription", sovereignDatasetDescription, SovereignDatasetDescriptionEncoder::encode);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public static MetadataReplicationMessage decode(DatasetStructDecoder datasetStructDecoder) {
    int index = datasetStructDecoder.int32(INDEX);

    StructDecoder<?> decoder;

    if ((decoder = datasetStructDecoder.getUnderlying().struct("persistableSchemaList")) != null) {
      return new MetadataReplicationMessage(index, PersistableSchemaListEncoder.decode(decoder));
    } else if ((decoder = datasetStructDecoder.getUnderlying().struct("cachingSequence")) != null) {
      return new MetadataReplicationMessage(index, CachingSequenceEncoder.decode(decoder));
    } else if ((decoder = datasetStructDecoder.getUnderlying().struct("sovereignDatasetDescription")) != null) {
      return new MetadataReplicationMessage(index, SovereignDatasetDescriptionEncoder.decode(decoder));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public ReplicationMessageType getType() {
    return ReplicationMessageType.METADATA_REPLICATION_MESSAGE;
  }
}
