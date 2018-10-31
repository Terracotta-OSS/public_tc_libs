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

package com.terracottatech.store.server.messages;

import com.terracottatech.sovereign.impl.dataset.metadata.SchemaCellDefinition;
import com.terracottatech.store.common.messages.StoreStructures;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import static com.terracottatech.store.common.messages.StoreStructures.CELL_DEFINITION_STRUCT;

public class SchemaCellDefinitionEncoder {

  private static final String DEF = "def";
  private static final String ID = "id";

  public static final Struct SCHEMA_CELL_DEFINITION = StructBuilder.newStructBuilder()
      .struct(DEF, 10, CELL_DEFINITION_STRUCT)
      .int32(ID, 20)
      .build();

  public static void encode(StructEncoder<?> encoder, SchemaCellDefinition<?> schemaCellDefinition) {
    encoder.struct(DEF, schemaCellDefinition.definition(), StoreStructures::encodeDefinition)
        .int32(ID, schemaCellDefinition.id());
  }

  public static SchemaCellDefinition<?> decode(StructDecoder<?> structDecoder) {
    return new SchemaCellDefinition<>(StoreStructures.decodeDefinition(structDecoder.struct(DEF)), structDecoder.int32(ID));
  }
}
