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
package com.terracottatech.store.common.messages;

import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.ClusteredDatasetConfiguration;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.messages.indexing.IndexingCodec;
import com.terracottatech.store.configuration.DiskDurability;
import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Map;

import static com.terracottatech.store.common.messages.StoreStructures.DURABILITY_STRUCT;
import static com.terracottatech.store.common.messages.StoreStructures.PERSISTENT_STORAGE_TYPE_ENUM_MAPPING;
import static com.terracottatech.store.common.messages.StoreStructures.TYPE_ENUM_MAPPING;
import static com.terracottatech.store.common.messages.StoreStructures.decodeDiskDurability;

public class ConfigurationEncoder {

  private static final Struct CLUSTERED_DATASET_CONFIGURATION_STRUCT = StructBuilder.newStructBuilder()
      .enm("keyType", 10, TYPE_ENUM_MAPPING)
      .string("datasetName", 20)
      .string("offheapResource", 30)
      .string("diskResource", 40)
      .structs("indexes", 50, IndexingCodec.INDEX_DEFINITION_STRUCT)
      .int32("concurrencyHint", 60)
      .struct("diskDurability", 70, DURABILITY_STRUCT)
      .enm("persistentStorageType", 80, PERSISTENT_STORAGE_TYPE_ENUM_MAPPING)
      .build();


  public static byte[] encode(DatasetEntityConfiguration<?> configuration) {
    Type<?> keyType = configuration.getKeyType();
    String datasetName = configuration.getDatasetName();
    ClusteredDatasetConfiguration datasetConfiguration = configuration.getDatasetConfiguration();
    String offheapResource = datasetConfiguration.getOffheapResource();
    Optional<String> diskResource = datasetConfiguration.getDiskResource();

    StructEncoder<Void> encoder = CLUSTERED_DATASET_CONFIGURATION_STRUCT.encoder();
    encoder.enm("keyType", keyType)
            .string("datasetName", datasetName)
            .string("offheapResource", offheapResource);

    diskResource.ifPresent(dr -> encoder.string("diskResource", dr));
    encoder.structs("indexes", datasetConfiguration.getIndexes().entrySet(), IndexingCodec::encodeIndexDefinition);
    datasetConfiguration.getConcurrencyHint().ifPresent(i -> encoder.int32("concurrencyHint", i));

    Optional<DiskDurability> diskDurability = datasetConfiguration.getDiskDurability();
    diskDurability.ifPresent(dd -> encoder.struct("diskDurability", dd, StoreStructures::encodeDiskDurability));

    Optional<PersistentStorageType> storageType = datasetConfiguration.getPersistentStorageType();
    storageType.ifPresent(st -> encoder.enm("persistentStorageType", st));

    return encoder
            .encode()
            .array();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static DatasetEntityConfiguration decode(byte[] bytes) {
    try {
      StructDecoder<?> decoder = CLUSTERED_DATASET_CONFIGURATION_STRUCT.decoder(ByteBuffer.wrap(bytes));

      Enm<Type<?>> keyType = decoder.enm("keyType");
      String datasetName = decoder.string("datasetName");
      String offheapResource = decoder.string("offheapResource");
      String diskResource = decoder.string("diskResource");
      Map<CellDefinition<?>, IndexSettings> indexes = IndexingCodec.decodeIndexDefinitions(decoder.structs("indexes"));
      Integer concurrencyHint = decoder.int32("concurrencyHint");

      DiskDurability diskDurability = decodeDiskDurability(decoder.struct("diskDurability"));

      Enm<PersistentStorageType> storageTypeEnm = decoder.enm("persistentStorageType");
      PersistentStorageType storageType = storageTypeEnm.isFound() ? storageTypeEnm.get() : null;

      ClusteredDatasetConfiguration datasetConfiguration = new ClusteredDatasetConfiguration(offheapResource,
          diskResource, indexes, concurrencyHint, diskDurability, storageType);

      return new DatasetEntityConfiguration(keyType.get(), datasetName, datasetConfiguration);
    } catch (RuntimeException e) {
      throw new StoreRuntimeException(e);
    }
  }
}
