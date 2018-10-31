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

import com.terracottatech.sovereign.impl.dataset.metadata.PersistableSchemaList;
import com.terracottatech.sovereign.impl.dataset.metadata.SchemaCellDefinition;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.store.server.messages.SchemaCellDefinitionEncoder.SCHEMA_CELL_DEFINITION;

public class PersistableSchemaListEncoder {

  private static final String SCHEMA_CELL_DEFINITION_LIST = "schemaCellDefinitionList";
  public static final Struct PERSISTABLE_SCHEMA_LIST_STRUCT = StructBuilder.newStructBuilder()
      .structs(SCHEMA_CELL_DEFINITION_LIST, 10, SCHEMA_CELL_DEFINITION)
      .build();

  public static void encode(StructEncoder<?> encoder, PersistableSchemaList persistableSchemaList) {
    List<SchemaCellDefinition<?>> list = persistableSchemaList.getDefinitions();
    encoder.structs(SCHEMA_CELL_DEFINITION_LIST, list, SchemaCellDefinitionEncoder::encode);
  }

  public static PersistableSchemaList decode(StructDecoder<?> decoder) {
    StructArrayDecoder<?> structArrayDecoder = decoder.structs(SCHEMA_CELL_DEFINITION_LIST);
    List<SchemaCellDefinition<?>> list = new ArrayList<>(structArrayDecoder.length());

    while (structArrayDecoder.hasNext()) {
      StructDecoder<?> structDecoder = structArrayDecoder.next();
      list.add(SchemaCellDefinitionEncoder.decode(structDecoder));
    }

    return new PersistableSchemaList(list);
  }
}
