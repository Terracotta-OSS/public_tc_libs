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
package com.terracottatech.store.client;

import com.terracottatech.store.Type;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.common.messages.ConfigurationEncoder;
import com.terracottatech.store.common.messages.DatasetOperationMessageCodec;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.MessageCodec;

public class DatasetEntityClientService
    implements EntityClientService<DatasetEntity<?>, DatasetEntityConfiguration<?>, DatasetEntityMessage, DatasetEntityResponse, DatasetEntity.Parameters> {

  @Override
  public boolean handlesEntityType(Class<DatasetEntity<?>> cls) {
    return DatasetEntity.class.isAssignableFrom(cls);
  }

  @Override
  public byte[] serializeConfiguration(DatasetEntityConfiguration<?> configuration) {
    return ConfigurationEncoder.encode(configuration);
  }

  @Override
  public DatasetEntityConfiguration<?> deserializeConfiguration(byte[] bytes) {
    return ConfigurationEncoder.decode(bytes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public DatasetEntity<?> create(EntityClientEndpoint<DatasetEntityMessage, DatasetEntityResponse> endpoint, DatasetEntity.Parameters datasetEntityParameters) {
    DatasetEntityConfiguration<?> configuration = deserializeConfiguration(endpoint.getEntityConfiguration());
    Type<?> keyType = configuration.getKeyType();
    return VoltronDatasetEntity.newInstance((Type) keyType, endpoint, datasetEntityParameters);
  }

  @Override
  public MessageCodec<DatasetEntityMessage, DatasetEntityResponse> getMessageCodec() {
    IntrinsicCodec intrinsicCodec = new IntrinsicCodec(ClientIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS);
    return new DatasetOperationMessageCodec(intrinsicCodec);
  }
}
