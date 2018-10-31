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

import com.terracottatech.store.common.messages.StoreStructures;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IndexingCodec {

  private static final EnumMapping<IndexSettings> INDEX_SETTINGS_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(IndexSettings.class)
          .mapping(IndexSettings.btree(), 0)
          .build();

  public static final Struct INDEX_DEFINITION_STRUCT = StructBuilder.newStructBuilder()
          .struct("cellDefinition", 10, StoreStructures.CELL_DEFINITION_STRUCT)
          .enm("indexSettings", 20, INDEX_SETTINGS_ENUM_MAPPING)
          .build();

  public static final EnumMapping<Index.Status> INDEX_STATUS_ENUM_MAPPING =
      EnumMappingBuilder.newEnumMappingBuilder(Index.Status.class)
          .mapping(Index.Status.LIVE, 0)
          .mapping(Index.Status.POTENTIAL, 1)
          .mapping(Index.Status.INITALIZING, 2)
          .mapping(Index.Status.DEAD, 3)
          .mapping(Index.Status.BROKEN, 4)
          .build();

  public static final Struct INDEX_STRUCT =
      StructBuilder.newStructBuilder()
          .struct("definition", 10, INDEX_DEFINITION_STRUCT)
          .enm("status", 20, INDEX_STATUS_ENUM_MAPPING)
          .build();

  public static void encodeIndexDefinition(StructEncoder<?> encoder, Map.Entry<CellDefinition<?>, IndexSettings> definition) {
    encoder.struct("cellDefinition", definition.getKey(), StoreStructures::encodeDefinition);
    encoder.enm("indexSettings", definition.getValue());
  }

  public static Map.Entry<CellDefinition<?>, IndexSettings> decodeIndexDefinition(StructDecoder<?> decoder) {
    CellDefinition<?> cell = StoreStructures.decodeDefinition(decoder.struct("cellDefinition"));
    IndexSettings settings = decoder.<IndexSettings>enm("indexSettings").get();
    return new AbstractMap.SimpleImmutableEntry<>(cell, settings);
  }

  public static Map<CellDefinition<?>, IndexSettings> decodeIndexDefinitions(StructArrayDecoder<?> decoder) {
    Map<CellDefinition<?>, IndexSettings> indexes = new HashMap<>();
    while(decoder.hasNext()) {
      Map.Entry<CellDefinition<?>, IndexSettings> index = decodeIndexDefinition(decoder.next());
      indexes.put(index.getKey(), index.getValue());
    }
    return Collections.unmodifiableMap(indexes);
  }
}
