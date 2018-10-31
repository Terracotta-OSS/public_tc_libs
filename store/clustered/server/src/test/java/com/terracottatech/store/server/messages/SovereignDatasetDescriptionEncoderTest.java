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

import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.SovereignType;
import com.terracottatech.sovereign.impl.indexing.SimpleIndex;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexing;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.sovereign.time.SystemTimeReference;
import org.hamcrest.beans.SamePropertyValuesAs;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Test;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.terracottatech.sovereign.indexing.SovereignIndexSettings.btree;
import static com.terracottatech.sovereign.indexing.SovereignIndexSettings.hash;
import static com.terracottatech.store.Type.BOOL;
import static com.terracottatech.store.Type.CHAR;
import static com.terracottatech.store.Type.DOUBLE;
import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.server.messages.SovereignDatasetDescriptionEncoder.SOVEREIGN_DATASET_DESCRIPTION_STRUCT;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SovereignDatasetDescriptionEncoderTest {

  @Test
  public void test() {
    generateSovereignDatasetDescriptions().forEach(original -> {
      SovereignDatasetDescription decoded = cycle(original);

      assertThat(decoded.getUUID(), is(original.getUUID()));
      assertThat(decoded.getAlias(), is(original.getAlias()));
      assertThat(decoded.getOffheapResourceName(), is(original.getOffheapResourceName()));
      assertThat(decoded.getIndexDescriptions(), IsIterableContainingInAnyOrder.containsInAnyOrder(original.getIndexDescriptions()
              .stream().map(idx -> SamePropertyValuesAs.<SovereignIndexDescription<?>>samePropertyValuesAs(idx)).collect(toList())));
    });
  }

  @SuppressWarnings("unchecked")
  private static Stream<SovereignDatasetDescription> generateSovereignDatasetDescriptions() {
    return generateSovereignDatasetConfigs().flatMap(config ->
            generateIndexing().map(indexing -> {
              SovereignDatasetImpl<String> dataset = mock(SovereignDatasetImpl.class);
              UUID uuid = generateUUID();

              when(dataset.getConfig()).thenReturn(config);
              when(dataset.getUUID()).thenReturn(uuid);
              when(dataset.getIndexing()).thenReturn(indexing);

              return new SovereignDatasetDescriptionImpl<>(dataset);
    }));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Stream<SovereignDataSetConfig> generateSovereignDatasetConfigs() {
    return of(BOOL, CHAR, INT, LONG, DOUBLE, STRING).flatMap(type ->
            of(FixedTimeReference.class, SystemTimeReference.class).map(time ->
                    new SovereignDataSetConfig(type, time)
            ));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Stream<SimpleIndexing<String>> generateIndexing() {
    List<SovereignIndex<?>> indexes =
            of(SovereignType.values()).flatMap(type ->
            of(SimpleIndex.State.values()).flatMap(state ->
            of(btree(), hash()).map(settings -> {
      SovereignIndexDescription<String> description = new SimpleIndexDescription(state, UUID.randomUUID().toString(), type.getNevadaType(), settings);
      SovereignIndex<String> index = mock(SovereignIndex.class);
      when(index.getDescription()).thenReturn(description);
      return index;
    }))).collect(toList());

    SimpleIndexing<String> indexing = mock(SimpleIndexing.class);
    when (indexing.getIndexes()).thenReturn(indexes);

    SimpleIndexing<String> emptyIndexing = mock(SimpleIndexing.class);
    when (indexing.getIndexes()).thenReturn(Collections.emptyList());
    return of(indexing, emptyIndexing);
  }

  private static UUID generateUUID() {
    return UUID.randomUUID();
  }

  public static SovereignDatasetDescription cycle(SovereignDatasetDescription description) {
    StructEncoder<?> encoder = SOVEREIGN_DATASET_DESCRIPTION_STRUCT.encoder();
    SovereignDatasetDescriptionEncoder.encode(encoder, description);
    ByteBuffer buffer = encoder.encode();

    buffer.flip();

    StructDecoder<?> decoder = SOVEREIGN_DATASET_DESCRIPTION_STRUCT.decoder(buffer);
    return SovereignDatasetDescriptionEncoder.decode(decoder);
  }
}