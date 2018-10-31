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
package com.terracottatech.store.common.messages.indexing;

import org.terracotta.runnel.Struct;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.UniversalMessage;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;

/**
 * Specifies a create index operation.
 */
public class IndexCreateMessage<T extends Comparable<T>> extends UniversalMessage {

  private final CellDefinition<T> cellDefinition;
  private final IndexSettings indexSettings;
  private final UUID stableClientId;

  public IndexCreateMessage(CellDefinition<T> cellDefinition, IndexSettings indexSettings, UUID stableClientId) {
    this.cellDefinition = cellDefinition;
    this.indexSettings = indexSettings;
    this.stableClientId = stableClientId;
  }

  public CellDefinition<T> getCellDefinition() {
    return cellDefinition;
  }

  public IndexSettings getIndexSettings() {
    return indexSettings;
  }

  public UUID getStableClientId() {
    return stableClientId;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.INDEX_CREATE_MESSAGE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("IndexCreateMessage{");
    sb.append("cellDefinition=").append(cellDefinition);
    sb.append(", indexSettings=").append(indexSettings);
    sb.append(", stableClientId=").append(stableClientId);
    sb.append('}');
    return sb.toString();
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    datasetStructBuilder
            .uuid("stableClientId", 10)
            .struct("definition", 20, IndexingCodec.INDEX_DEFINITION_STRUCT);
    return datasetStructBuilder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    IndexCreateMessage<?> createMessage = (IndexCreateMessage)message;
    Map.Entry<CellDefinition<?>, IndexSettings> indexDefinition = new AbstractMap.SimpleImmutableEntry<>(
            createMessage.getCellDefinition(), createMessage.getIndexSettings());
    encoder.uuid("stableClientId", createMessage.getStableClientId())
            .struct("definition",
        indexDefinition,
        (structEncoder, definition) -> IndexingCodec.encodeIndexDefinition(structEncoder.getUnderlying(), definition));
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> IndexCreateMessage<T> decode(DatasetStructDecoder decoder) {
    UUID stableClientId = decoder.uuid("stableClientId");
    Map.Entry<CellDefinition<?>, IndexSettings> definition =
        IndexingCodec.decodeIndexDefinition(decoder.getUnderlying().struct("definition"));
    return new IndexCreateMessage<>(((CellDefinition<T>)definition.getKey()), definition.getValue(), stableClientId);
  }
}
