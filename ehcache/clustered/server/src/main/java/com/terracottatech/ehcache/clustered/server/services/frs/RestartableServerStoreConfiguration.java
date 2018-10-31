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
package com.terracottatech.ehcache.clustered.server.services.frs;

import com.terracottatech.ehcache.clustered.common.internal.messages.EnterpriseConfigCodec;
import com.terracottatech.ehcache.common.frs.LocalEncoder;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.ChildConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataConfiguration;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;

/**
 * Metadata for each server store.
 * <p>
 *   Currently even if each server store cannot be in different restart log, the flexibility is
 *   built into the metadata system for future expansion.
 * @author vmad
 */
@CommonComponent
public class RestartableServerStoreConfiguration implements ChildConfiguration<String>, LocalEncoder {
  // TODO: Even store encoders/decoders can use runnel
  private static final String INDEX_ID = "index";
  private static final String PARENT_ID = "parentId";
  private static final String DATA_LOG_ID = "dataLogIdentifier";
  private static final EnterpriseConfigCodec ENTERPRISE_CONFIG_CODEC = new EnterpriseConfigCodec();
  private static final Struct CONFIG_STRUCT;

  static {
    StructBuilder structBuilder = StructBuilder.newStructBuilder()
      .int32(INDEX_ID, 10)
      .string(PARENT_ID, 20)
      .struct(DATA_LOG_ID, 30, FrsDataLogIdentifier.STRUCT);
    CONFIG_STRUCT = ENTERPRISE_CONFIG_CODEC.injectServerStoreConfiguration(structBuilder, 40).getUpdatedBuilder().build();
  }
  private int index;
  private final String parentId;
  private final ServerStoreConfiguration serverStoreConfiguration;
  private final FrsDataLogIdentifier dataLogIdentifier;

  public RestartableServerStoreConfiguration(String parentId,
                                      ServerStoreConfiguration serverStoreConfiguration,
                                      RestartConfiguration restartConfiguration) {
    this.parentId = parentId;
    this.serverStoreConfiguration = serverStoreConfiguration;
    this.dataLogIdentifier = new FrsDataLogIdentifier(restartConfiguration.getRestartableLogRoot(),
        restartConfiguration.getRestartableLogContainer(),
        restartConfiguration.getRestartableLogName());
  }

  //this constructor is needed for LocalEncoder support
  public RestartableServerStoreConfiguration(ByteBuffer byteBuffer) {
    StructDecoder<Void> structDecoder = CONFIG_STRUCT.decoder(byteBuffer);
    this.index = structDecoder.int32(INDEX_ID);
    this.parentId = structDecoder.string(PARENT_ID);
    this.dataLogIdentifier = FrsDataLogIdentifier.decode(structDecoder.struct(DATA_LOG_ID));
    this.serverStoreConfiguration = ENTERPRISE_CONFIG_CODEC.decodeServerStoreConfiguration(structDecoder);
  }

  @Override
  public String getParentId() {
    return parentId;
  }

  @Override
  public FrsDataLogIdentifier getDataLogIdentifier() {
    return dataLogIdentifier;
  }

  @Override
  public int getObjectIndex() {
    return index;
  }

  @Override
  public void setObjectIndex(int index) {
    this.index = index;
  }

  @Override
  public void validate(MetadataConfiguration newMetadataConfiguration) {
    if (newMetadataConfiguration instanceof RestartableServerStoreConfiguration) {
      ServerStoreConfiguration otherServerStoreConfiguration = ((RestartableServerStoreConfiguration) newMetadataConfiguration).getServerStoreConfiguration();
      StringBuilder errorBuilder = new StringBuilder();
      if (!this.serverStoreConfiguration.isCompatible(otherServerStoreConfiguration, errorBuilder)) {
        throw new IllegalArgumentException(errorBuilder.toString());
      }
      return;
    }
    throw new IllegalArgumentException("Unexpected configuration type: " + newMetadataConfiguration);
  }

  public ServerStoreConfiguration getServerStoreConfiguration() {
    return serverStoreConfiguration;
  }

  @Override
  public ByteBuffer encode() {
    StructEncoder<Void> structEncoder = CONFIG_STRUCT.encoder()
      .int32(INDEX_ID, getObjectIndex())
      .string(PARENT_ID, getParentId())
      .struct(DATA_LOG_ID, getDataLogIdentifier(), (encoder, identifier) -> identifier.encode(encoder));
    ENTERPRISE_CONFIG_CODEC.encodeServerStoreConfiguration(structEncoder, getServerStoreConfiguration());
    ByteBuffer encodedBytes = structEncoder.encode();
    encodedBytes.flip();
    return encodedBytes;
  }
}