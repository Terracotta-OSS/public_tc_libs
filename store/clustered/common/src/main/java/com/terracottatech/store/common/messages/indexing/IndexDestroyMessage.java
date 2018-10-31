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

/**
 * Specifies a create index operation.
 */
public class IndexDestroyMessage<T extends Comparable<T>> extends UniversalMessage {

  private final CellDefinition<T> cellDefinition;
  private final IndexSettings indexSettings;

  public IndexDestroyMessage(CellDefinition<T> cellDefinition, IndexSettings indexSettings) {
    this.cellDefinition = cellDefinition;
    this.indexSettings = indexSettings;
  }

  public CellDefinition<T> getCellDefinition() {
    return cellDefinition;
  }

  public IndexSettings getIndexSettings() {
    return indexSettings;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.INDEX_DESTROY_MESSAGE;
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    datasetStructBuilder.getUnderlying()
        .struct("definition", 10, IndexingCodec.INDEX_DEFINITION_STRUCT);
    return datasetStructBuilder.build();
  }

  public static <T extends Comparable<T>> void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    @SuppressWarnings("unchecked") IndexDestroyMessage<T> destroyMessage = (IndexDestroyMessage<T>)message;
    Map.Entry<CellDefinition<?>, IndexSettings> indexDefinition =
        new AbstractMap.SimpleImmutableEntry<>(destroyMessage.getCellDefinition(), destroyMessage.getIndexSettings());
    encoder.struct("definition",
        indexDefinition,
        (structEncoder, definition) -> IndexingCodec.encodeIndexDefinition(structEncoder.getUnderlying(), definition));
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> IndexDestroyMessage<T> decode(DatasetStructDecoder decoder) {
    Map.Entry<CellDefinition<?>, IndexSettings> definition =
        IndexingCodec.decodeIndexDefinition(decoder.getUnderlying().struct("definition"));
    return new IndexDestroyMessage<>(((CellDefinition<T>)definition.getKey()), definition.getValue());
  }
}
