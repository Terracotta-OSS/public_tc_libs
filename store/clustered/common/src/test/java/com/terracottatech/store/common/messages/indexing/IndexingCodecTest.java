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

import org.junit.Test;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Basic tests for {@link IndexingCodec}.
 */
public class IndexingCodecTest {

  @Test
  public void testIndexDefinition() throws Exception {
    CellDefinition<?> cellDefinition = CellDefinition.defineString("someCell");
    IndexSettings indexSettings = IndexSettings.BTREE;
    Map.Entry<CellDefinition<?>, IndexSettings> definition =
        new AbstractMap.SimpleImmutableEntry<>(cellDefinition, indexSettings);

    StructEncoder<Void> encoder = IndexingCodec.INDEX_DEFINITION_STRUCT.encoder();
    IndexingCodec.encodeIndexDefinition(encoder, definition);
    ByteBuffer buffer = encoder.encode();
    buffer.rewind();

    StructDecoder<Void> decoder = IndexingCodec.INDEX_DEFINITION_STRUCT.decoder(buffer);
    Map.Entry<CellDefinition<?>, IndexSettings> decodedDefinition = IndexingCodec.decodeIndexDefinition(decoder);

    assertThat(decodedDefinition, is(definition));
  }
}