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
import com.terracottatech.ehcache.common.frs.LocalEncoder;
import com.terracottatech.ehcache.common.frs.metadata.ChildConfiguration;
import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import com.terracottatech.ehcache.common.frs.metadata.MetadataConfiguration;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;

import java.nio.ByteBuffer;

/**
 * @author vmad
 */
@CommonComponent
public class RestartableStateRepositoryConfig implements ChildConfiguration<String>, LocalEncoder {

  private static final String INDEX_ID = "index";
  private static final String PARENT_ID = "parentId";
  private static final String DATA_LOG_ID = "frsDataLogIdentifier";
  private static final Struct CONFIG_STRUCT = StructBuilder.newStructBuilder()
    .int32(INDEX_ID, 10)
    .string(PARENT_ID, 20)
    .struct(DATA_LOG_ID, 30, FrsDataLogIdentifier.STRUCT).build();

  private int index;
  private final String parentId;
  private final FrsDataLogIdentifier frsDataLogIdentifier;

  public RestartableStateRepositoryConfig(String parentId, FrsDataLogIdentifier frsDataLogIdentifier) {
    this.parentId = parentId;
    this.frsDataLogIdentifier = frsDataLogIdentifier;
  }

  //this constructor is needed for LocalEncoder support
  public RestartableStateRepositoryConfig(ByteBuffer byteBuffer) {
    StructDecoder<Void> structDecoder = CONFIG_STRUCT.decoder(byteBuffer);
    this.index = structDecoder.int32(INDEX_ID);
    this.parentId = structDecoder.string(PARENT_ID);
    this.frsDataLogIdentifier = FrsDataLogIdentifier.decode(structDecoder.struct(DATA_LOG_ID));
  }

  @Override
  public String getParentId() {
    return parentId;
  }

  @Override
  public FrsDataLogIdentifier getDataLogIdentifier() {
    return frsDataLogIdentifier;
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
    //nothing to validate, all we have is dummy config
  }

  @Override
  public ByteBuffer encode() {
    ByteBuffer encodedBytes = CONFIG_STRUCT.encoder()
      .int32(INDEX_ID, getObjectIndex())
      .string(PARENT_ID, getParentId())
      .struct(DATA_LOG_ID, getDataLogIdentifier(), (encoder, identifer) -> identifer.encode(encoder)).encode();
    encodedBytes.flip();
    return encodedBytes;
  }
}