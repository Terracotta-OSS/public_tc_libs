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
package com.terracottatech.store.coordination;

import com.terracottatech.store.common.coordination.CoordinationMessage;
import com.terracottatech.store.common.coordination.CoordinatorMessaging;
import com.terracottatech.store.common.coordination.StateResponse;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.MessageCodec;

public class CoordinationEntityClientService implements EntityClientService<CoordinationEntity, Void, CoordinationMessage, StateResponse, Object[]> {
  @Override
  public boolean handlesEntityType(Class<CoordinationEntity> cls) {
    return CoordinationEntity.class.isAssignableFrom(cls);
  }

  @Override
  public byte[] serializeConfiguration(Void configuration) {
    return new byte[0];
  }

  @Override
  public Void deserializeConfiguration(byte[] configuration) {
    return null;
  }

  @Override
  public CoordinationEntity create(EntityClientEndpoint<CoordinationMessage, StateResponse> endpoint, Object[] configuration) {
    try {
      return new CoordinationEntityImpl(endpoint, configuration);
    } catch (Throwable t) {
      try {
        endpoint.close();
      } catch (Throwable u) {
        t.addSuppressed(u);
      }
      throw t;
    }
  }

  @Override
  public MessageCodec<CoordinationMessage, StateResponse> getMessageCodec() {
    return CoordinatorMessaging.codec();
  }
}
