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
import com.terracottatech.store.server.messages.ServerServerMessage;
import com.terracottatech.store.server.messages.ServerServerMessageType;
import com.terracottatech.store.server.messages.SovereignDatasetDescriptionEncoder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.decoding.StructDecoder;

import static com.terracottatech.store.server.messages.CachingSequenceEncoder.CACHING_SEQUENCE_STRUCT;
import static com.terracottatech.store.server.messages.PersistableSchemaListEncoder.PERSISTABLE_SCHEMA_LIST_STRUCT;
import static com.terracottatech.store.server.messages.SovereignDatasetDescriptionEncoder.SOVEREIGN_DATASET_DESCRIPTION_STRUCT;

public class MetadataSyncMessage extends PassiveSyncMessage {

  private final Object metadata;
  private final int tagKey;

  public MetadataSyncMessage(Object metadata, int tagKey) {
    this.metadata = metadata;
    this.tagKey = tagKey;
  }

  public int getTagKey() {
    return tagKey;
  }

  public Object getMetadata() {
    return metadata;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.getUnderlying().int32("tagKey", 10)
        .struct("persistableSchemaList", 20, PERSISTABLE_SCHEMA_LIST_STRUCT)
        .struct("cachingSequence", 30, CACHING_SEQUENCE_STRUCT)
        .struct("sovereignDatasetDescription", 40, SOVEREIGN_DATASET_DESCRIPTION_STRUCT)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, ServerServerMessage message) {
    MetadataSyncMessage metadataSyncMessage = (MetadataSyncMessage) message;
    encoder.int32("tagKey", metadataSyncMessage.getTagKey());

    Object metadata = metadataSyncMessage.getMetadata();

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

  public static MetadataSyncMessage decode(DatasetStructDecoder decoder) {
    int tagKey = decoder.int32("tagKey");

    StructDecoder<?> structDecoder;

    if ((structDecoder = decoder.getUnderlying().struct("persistableSchemaList")) != null) {
      return new MetadataSyncMessage(PersistableSchemaListEncoder.decode(structDecoder), tagKey);
    } else if ((structDecoder = decoder.getUnderlying().struct("cachingSequence")) != null) {
      return new MetadataSyncMessage(CachingSequenceEncoder.decode(structDecoder), tagKey);
    } else if ((structDecoder = decoder.getUnderlying().struct("sovereignDatasetDescription")) != null) {
      return new MetadataSyncMessage(SovereignDatasetDescriptionEncoder.decode(structDecoder), tagKey);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public ServerServerMessageType getType() {
    return ServerServerMessageType.METADATA_SYNC_MESSAGE;
  }
}
