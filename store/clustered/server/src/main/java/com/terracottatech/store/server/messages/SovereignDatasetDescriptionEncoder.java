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
import com.terracottatech.store.common.ByteBufferUtil;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.Arrays;
import java.util.List;

public class SovereignDatasetDescriptionEncoder {

  public static final Struct SOVEREIGN_DATASET_DESCRIPTION_STRUCT = StructBuilder.newStructBuilder()
      .byteBuffer("data", 10)
      .build();

  public static void encode(StructEncoder<?> encoder, SovereignDatasetDescription sovereignDatasetDescription) {
    encoder.byteBuffer("data", ByteBufferUtil.serialize(sovereignDatasetDescription));
  }

  public static SovereignDatasetDescription decode(StructDecoder<?> structDecoder) {
    return ByteBufferUtil.deserializeWhiteListed(structDecoder.byteBuffer("data"), SovereignDatasetDescription.class, WHITELIST);
  }

  private static List<Class<?>> WHITELIST = Arrays.asList(
          com.terracottatech.sovereign.impl.ConcurrencyFormulator.class,
          com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl.class,
          com.terracottatech.sovereign.impl.SovereignDataSetConfig.class,
          com.terracottatech.sovereign.impl.SovereignDataSetConfig.RecordBufferStrategyType.class,
          com.terracottatech.sovereign.impl.SovereignDataSetConfig.StorageType.class,
          com.terracottatech.sovereign.impl.SovereignDataSetConfig.VersionLimitStrategyImpl.class,
          com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription.class,
          com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription[].class,
          com.terracottatech.sovereign.indexing.SovereignIndex.State.class,
          com.terracottatech.sovereign.indexing.SovereignIndexSettings.BTREESovereignIndexSettings.class,
          com.terracottatech.sovereign.indexing.SovereignIndexSettings.HASHSovereignIndexSettings.class,
          com.terracottatech.sovereign.time.FixedTimeReference.class,
          com.terracottatech.sovereign.time.SystemTimeReference.class,
          com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability.class,
          com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability.DurabilityEnum.class,
          com.terracottatech.sovereign.impl.SovereignDatasetDiskDurability.Timed.class,

          java.lang.Boolean.class,
          java.lang.Character.class,
          java.lang.Double.class,
          java.lang.Enum.class,
          java.lang.Integer.class,
          java.lang.Long.class,
          java.lang.Number.class,
          java.lang.String.class,
          java.util.UUID.class,

          byte[].class
  );
}
