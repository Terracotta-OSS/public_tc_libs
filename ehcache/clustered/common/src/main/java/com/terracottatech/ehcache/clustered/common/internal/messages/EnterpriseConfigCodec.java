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
package com.terracottatech.ehcache.clustered.common.internal.messages;

import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.messages.CommonConfigCodec;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.PrimitiveDecodingSupport;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.PrimitiveEncodingSupport;
import org.terracotta.runnel.encoding.StructEncoder;

import com.terracottatech.ehcache.clustered.common.EnterprisePoolAllocation;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartableOffHeapMode;

import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;

/**
 * Encodes and decodes enterprise version of configuration objects such as
 * {@link ServerSideConfiguration} and {@link ServerStoreConfiguration}.
 */
public class EnterpriseConfigCodec extends CommonConfigCodec {

  private static final String STORE_CONFIG_RESTARTABLE = "restartable";
  private static final String STORE_CONFIG_CACHE_PERCENT = "dataPercent";

  private static final String RESTART_CONFIG_LOG_ROOT_FIELD = "restartableLogRoot";
  private static final String RESTART_CONFIG_OFF_HEAP_MODE_FIELD = "offHeapMode";
  private static final String RESTART_CONFIG_MGR_FRS_FIELD = "cacheManagerFrsId";

  private static final EnumMapping<RestartableOffHeapMode> OFF_HEAP_MODE_ENUM_MAPPING =
      newEnumMappingBuilder(RestartableOffHeapMode.class)
          .mapping(RestartableOffHeapMode.FULL, 1)
          .mapping(RestartableOffHeapMode.PARTIAL, 2)
          .build();

  @Override
  public InjectTuple injectServerStoreConfiguration(StructBuilder baseBuilder, final int index) {
    final InjectTuple baseTuple = super.injectServerStoreConfiguration(baseBuilder, index);
    final StructBuilder newBuilder = baseTuple.getUpdatedBuilder();
    final int newIndex = baseTuple.getLastIndex();

    final StructBuilder structBuilder = newBuilder.bool(STORE_CONFIG_RESTARTABLE, newIndex + 10)
                                                  .int32(STORE_CONFIG_CACHE_PERCENT, newIndex + 20);

    return new InjectTuple() {
      @Override
      public int getLastIndex() {
        return newIndex + 20;
      }

      @Override
      public StructBuilder getUpdatedBuilder() {
        return structBuilder;
      }
    };
  }

  @Override
  public InjectTuple injectServerSideConfiguration(StructBuilder baseBuilder, final int index) {
    final InjectTuple baseTuple = super.injectServerSideConfiguration(baseBuilder, index);
    final StructBuilder newBuilder = baseTuple.getUpdatedBuilder();
    final int newIndex = baseTuple.getLastIndex();

    final StructBuilder structBuilder = newBuilder.string(RESTART_CONFIG_LOG_ROOT_FIELD, newIndex + 10)
        .enm(RESTART_CONFIG_OFF_HEAP_MODE_FIELD, newIndex + 20, OFF_HEAP_MODE_ENUM_MAPPING)
        .string(RESTART_CONFIG_MGR_FRS_FIELD, newIndex + 30);

    return new InjectTuple() {
      @Override
      public int getLastIndex() {
        return newIndex + 30;
      }

      @Override
      public StructBuilder getUpdatedBuilder() {
        return structBuilder;
      }
    };
  }

  @Override
  public void encodeServerStoreConfiguration(PrimitiveEncodingSupport<?> encoder, ServerStoreConfiguration configuration) {
    super.encodeServerStoreConfiguration(encoder, configuration);

    PoolAllocation poolAllocation = configuration.getPoolAllocation();
    if (poolAllocation instanceof EnterprisePoolAllocation.DedicatedRestartable) {
      EnterprisePoolAllocation.DedicatedRestartable dedicated = (EnterprisePoolAllocation.DedicatedRestartable)poolAllocation;
      encoder.int64("poolSize", dedicated.getSize());
      if (dedicated.getResourceName() != null) {
        encoder.string("resourceName", dedicated.getResourceName());
      }
      encoder.bool(STORE_CONFIG_RESTARTABLE, true);
      encoder.int32(STORE_CONFIG_CACHE_PERCENT, dedicated.getDataPercent());
    } else if (poolAllocation instanceof EnterprisePoolAllocation.SharedRestartable) {
      EnterprisePoolAllocation.SharedRestartable shared = (EnterprisePoolAllocation.SharedRestartable)poolAllocation;
      encoder.string("resourceName", shared.getResourcePoolName());
      encoder.bool(STORE_CONFIG_RESTARTABLE, true);
    }
  }

  @Override
  public ServerStoreConfiguration decodeServerStoreConfiguration(PrimitiveDecodingSupport decoder) {
    final ServerStoreConfiguration baseConfiguration = super.decodeServerStoreConfiguration(decoder);

    final Boolean restartable = decoder.bool(STORE_CONFIG_RESTARTABLE);


    PoolAllocation newAllocation = new PoolAllocation.Unknown();
    if (restartable != null) {
      final PoolAllocation allocation = baseConfiguration.getPoolAllocation();
      if (allocation instanceof  PoolAllocation.Dedicated) {
        final Integer valuesPercent = decoder.int32(STORE_CONFIG_CACHE_PERCENT);
        final PoolAllocation.Dedicated dedicated = (PoolAllocation.Dedicated)allocation;
        newAllocation = new EnterprisePoolAllocation.DedicatedRestartable(dedicated.getResourceName(),
            dedicated.getSize(), valuesPercent);
      } else if (allocation instanceof PoolAllocation.Shared) {
        final PoolAllocation.Shared shared = (PoolAllocation.Shared)allocation;
        newAllocation = new EnterprisePoolAllocation.SharedRestartable(shared.getResourcePoolName());
      }
      return new ServerStoreConfiguration(newAllocation, baseConfiguration.getStoredKeyType(),
          baseConfiguration.getStoredValueType(), baseConfiguration.getKeySerializerType(),
          baseConfiguration.getValueSerializerType(), baseConfiguration.getConsistency());
    } else {
      return baseConfiguration;
    }
  }

  @Override
  public void encodeServerSideConfiguration(StructEncoder<?> encoder, ServerSideConfiguration configuration) {
    super.encodeServerSideConfiguration(encoder, configuration);
    if (configuration instanceof EnterpriseServerSideConfiguration) {
      final RestartConfiguration restartConfig = ((EnterpriseServerSideConfiguration)configuration).getRestartConfiguration();
      encoder.string(RESTART_CONFIG_LOG_ROOT_FIELD, restartConfig.getRestartableLogRoot());
      encoder.enm(RESTART_CONFIG_OFF_HEAP_MODE_FIELD, restartConfig.getOffHeapMode());
      encoder.string(RESTART_CONFIG_MGR_FRS_FIELD, restartConfig.getFrsIdentifier());
    }
  }

  @Override
  public ServerSideConfiguration decodeServerSideConfiguration(StructDecoder<?> decoder) {
    final ServerSideConfiguration baseConfig = super.decodeServerSideConfiguration(decoder);
    final String logRoot = decoder.string(RESTART_CONFIG_LOG_ROOT_FIELD);
    if (logRoot != null) {
      RestartableOffHeapMode offHeapMode = RestartableOffHeapMode.FULL;
      final Enm<?> offHeapModeField = decoder.enm(RESTART_CONFIG_OFF_HEAP_MODE_FIELD);
      if (offHeapModeField.isValid()) {
        offHeapMode = (RestartableOffHeapMode)offHeapModeField.get();
      }
      final String cacheManagerFrsName = decoder.string(RESTART_CONFIG_MGR_FRS_FIELD);
      final RestartConfiguration restartConfig = new RestartConfiguration(logRoot, offHeapMode, cacheManagerFrsName);
      return (baseConfig.getDefaultServerResource() != null) ?
          new EnterpriseServerSideConfiguration(baseConfig.getDefaultServerResource(), baseConfig.getResourcePools(), restartConfig) :
          new EnterpriseServerSideConfiguration(baseConfig.getResourcePools(), restartConfig);
    } else {
      return baseConfig;
    }
  }
}