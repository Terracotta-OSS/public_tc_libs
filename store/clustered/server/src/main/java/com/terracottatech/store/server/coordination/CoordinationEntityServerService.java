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
package com.terracottatech.store.server.coordination;

import com.terracottatech.store.common.coordination.CoordinationMessage;
import com.terracottatech.store.common.coordination.CoordinatorMessaging;
import com.terracottatech.store.common.coordination.StateResponse;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.NoConcurrencyStrategy;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

import java.util.Collections;
import java.util.Set;

import static java.util.Collections.emptySet;

public class CoordinationEntityServerService implements EntityServerService<CoordinationMessage, StateResponse> {
  @Override
  public long getVersion() {
    return 1L;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return "com.terracottatech.store.coordination.CoordinationEntity".equals(typeName);
  }

  @Override
  public ActiveServerEntity<CoordinationMessage, StateResponse> createActiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    try {
      return new CoordinationActiveEntity(registry.getService(() -> ClientCommunicator.class));
    } catch (ServiceException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public PassiveServerEntity<CoordinationMessage, StateResponse> createPassiveEntity(ServiceRegistry registry, byte[] configuration) throws ConfigurationException {
    return new CoordinationPassiveEntity();
  }

  @Override
  public ConcurrencyStrategy<CoordinationMessage> getConcurrencyStrategy(byte[] configuration) {
    return new ConcurrencyStrategy<CoordinationMessage>() {
      @Override
      public int concurrencyKey(CoordinationMessage message) {
        return 1;
      }

      @Override
      public Set<Integer> getKeysForSynchronization() {
        //No passive state == no passive sync
        return emptySet();
      }
    };
  }

  @Override
  public ExecutionStrategy<CoordinationMessage> getExecutionStrategy(byte[] configuration) {
    return message -> ExecutionStrategy.Location.ACTIVE;
  }

  @Override
  public MessageCodec<CoordinationMessage, StateResponse> getMessageCodec() {
    return CoordinatorMessaging.codec();
  }

  @Override
  public SyncMessageCodec<CoordinationMessage> getSyncMessageCodec() {
    return new SyncMessageCodec<CoordinationMessage>() {
      @Override
      public byte[] encode(int concurrencyKey, CoordinationMessage response) throws MessageCodecException {
        throw new IllegalStateException();
      }

      @Override
      public CoordinationMessage decode(int concurrencyKey, byte[] payload) throws MessageCodecException {
        throw new IllegalStateException();
      }
    };
  }
}
