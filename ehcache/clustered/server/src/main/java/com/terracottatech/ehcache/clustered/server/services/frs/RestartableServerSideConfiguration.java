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

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.clustered.common.EnterpriseServerSideConfiguration;
import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.common.internal.messages.EnterpriseConfigCodec;
import com.terracottatech.ehcache.common.frs.LocalEncoder;
import com.terracottatech.ehcache.common.frs.metadata.FrsContainerIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.RootConfiguration;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructArrayDecoder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author vmad
 */
@CommonComponent
public class RestartableServerSideConfiguration implements RootConfiguration, LocalEncoder {
  private static final String INDEX_ID = "index";
  private static final String DESTROY_FLAG = "destroyInProgress";
  private static final String FRS_CONTAINER_IDENTIFIERS_ID = "frsContainerIdentifiers";
  private static final EnterpriseConfigCodec ENTERPRISE_CONFIG_CODEC = new EnterpriseConfigCodec();
  private static final Struct CONFIG_STRUCT;

  static {
    StructBuilder structBuilder = StructBuilder.newStructBuilder()
      .int32(INDEX_ID, 10)
      .bool(DESTROY_FLAG, 15)
      .structs(FRS_CONTAINER_IDENTIFIERS_ID, 20, FrsContainerIdentifier.STRUCT);
    CONFIG_STRUCT = ENTERPRISE_CONFIG_CODEC.injectServerSideConfiguration(structBuilder, 30).getUpdatedBuilder().build();
  }

  private final EnterpriseServerSideConfiguration serverSideConfiguration;
  private final Set<FrsContainerIdentifier> frsContainerIdentifiers = new HashSet<>();
  private int index;
  private boolean destroyInProgress;

  public RestartableServerSideConfiguration(EnterpriseServerSideConfiguration serverSideConfiguration) {
    this.serverSideConfiguration = serverSideConfiguration;
    final RestartConfiguration restartConfig = serverSideConfiguration.getRestartConfiguration();
    final FrsContainerIdentifier containerId = new FrsContainerIdentifier(restartConfig.getRestartableLogRoot(),
        restartConfig.getRestartableLogContainer());
    // currently there is only one container, but still keep it as a set for future expansion
    frsContainerIdentifiers.add(containerId);
  }

  //this constructor is needed for LocalEncoder support
  public RestartableServerSideConfiguration(ByteBuffer byteBuffer) {
    StructDecoder<Void> structDecoder = CONFIG_STRUCT.decoder(byteBuffer);
    this.index = structDecoder.int32(INDEX_ID);
    this.destroyInProgress = structDecoder.bool(DESTROY_FLAG);
    StructArrayDecoder<? extends StructDecoder<?>> entriesDecoder = structDecoder.structs(FRS_CONTAINER_IDENTIFIERS_ID);
    for(int i = 0; i < entriesDecoder.length(); i++) {
      this.frsContainerIdentifiers.add(FrsContainerIdentifier.decode(entriesDecoder.next()));
    }
    this.serverSideConfiguration = (EnterpriseServerSideConfiguration) ENTERPRISE_CONFIG_CODEC.decodeServerSideConfiguration(structDecoder);
  }

  @Override
  public Set<FrsContainerIdentifier> getContainers() {
    return Collections.unmodifiableSet(frsContainerIdentifiers);
  }

  @Override
  public int getObjectIndex() {
    return index;
  }

  @Override
  public void setObjectIndex(int index) {
    this.index = index;
  }

  public boolean isDestroyInProgress() {
    return destroyInProgress;
  }

  public void setDestroyInProgress(final boolean destroyInProgress) {
    this.destroyInProgress = destroyInProgress;
  }

  @Override
  public void validate(MetadataConfiguration newMetadataConfiguration) {
    if (newMetadataConfiguration instanceof RestartableServerSideConfiguration) {
      RestartableServerSideConfiguration otherRestartableServerSideConfig =
          (RestartableServerSideConfiguration) newMetadataConfiguration;
      EnterpriseServerSideConfiguration otherServerSideConfiguration =
          otherRestartableServerSideConfig.getServerSideConfiguration();
      String defaultServerResource = this.serverSideConfiguration.getDefaultServerResource();
      String otherDefaultServerResource = otherServerSideConfiguration.getDefaultServerResource();
      if((defaultServerResource == null && otherDefaultServerResource != null) ||
        (defaultServerResource != null && !defaultServerResource.equals(otherDefaultServerResource)) ||
        !this.serverSideConfiguration.getResourcePools().equals(otherServerSideConfiguration.getResourcePools())) {
        throw new IllegalArgumentException("Configuration mismatch!");
      }
      this.serverSideConfiguration.getRestartConfiguration().checkCompatibility(
          otherServerSideConfiguration.getRestartConfiguration(), false);
    } else {
      throw new IllegalArgumentException("Unexpected configuration type: " + newMetadataConfiguration.getClass());
    }
  }

  public EnterpriseServerSideConfiguration getServerSideConfiguration() {
    return serverSideConfiguration;
  }

  @Override
  public ByteBuffer encode() {
    StructEncoder<Void> structEncoder = CONFIG_STRUCT.encoder()
      .int32(INDEX_ID, getObjectIndex())
      .bool(DESTROY_FLAG, isDestroyInProgress())
      .structs(FRS_CONTAINER_IDENTIFIERS_ID, frsContainerIdentifiers, (encoderStructArrayEncoder, frsContainerIdentifier) -> frsContainerIdentifier.encode(encoderStructArrayEncoder));
    ENTERPRISE_CONFIG_CODEC.encodeServerSideConfiguration(structEncoder, getServerSideConfiguration());
    ByteBuffer encodedBytes = structEncoder.encode();
    encodedBytes.flip();
    return encodedBytes;
  }
}
