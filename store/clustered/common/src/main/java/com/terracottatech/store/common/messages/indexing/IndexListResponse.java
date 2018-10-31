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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

import org.terracotta.runnel.Struct;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.terracottatech.store.common.messages.DatasetEntityResponseType.INDEX_LIST_RESPONSE;

public class IndexListResponse extends DatasetEntityResponse {

  private final Collection<Index<?>> indexes;

  public IndexListResponse(Collection<Index<?>> indexes) {
    this.indexes = indexes;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return INDEX_LIST_RESPONSE;
  }

  public Collection<Index<?>> getIndexes() {
    return indexes;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.getUnderlying().structs("indexes", 10, IndexingCodec.INDEX_STRUCT).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    IndexListResponse listResponse = (IndexListResponse) response;
    encoder.structs("indexes", listResponse.getIndexes(), (elementEncoder, index) -> {
      IndexingCodec.encodeIndexDefinition(elementEncoder.getUnderlying().struct("definition"),
          new AbstractMap.SimpleImmutableEntry<>(index.on(), index.definition()));
      elementEncoder.enm("status", index.status());
    });
  }

  public static IndexListResponse decode(DatasetStructDecoder decoder) {
    List<Index<?>> indexes = decoder.structs("indexes", IndexListResponse::getIndexEntry);
    return new IndexListResponse(indexes);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Comparable<T>> Index<T> getIndexEntry(DatasetStructDecoder elementDecoder) {
    Map.Entry<CellDefinition<?>, IndexSettings> definition =
        IndexingCodec.decodeIndexDefinition(elementDecoder.getUnderlying().struct("definition"));
    Index.Status status = elementDecoder.<Index.Status>enm("status").get();
    return new ImmutableIndex<>(((CellDefinition<T>)definition.getKey()), definition.getValue(), status);
  }
}
