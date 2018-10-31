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
package com.terracottatech.testing.calculator;

import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;

import java.util.Collections;
import java.util.Set;

/**
 *
 */
public class CalculatorEntityServerService implements EntityServerService<CalculatorReq, CalculatorRsp> {

  private static final ConcurrencyStrategy<CalculatorReq> CONCURRENCY = new ConcurrencyStrategy<CalculatorReq>() {
    @Override
    public int concurrencyKey(CalculatorReq message) {
      return ConcurrencyStrategy.MANAGEMENT_KEY;
    }

    @Override
    public Set<Integer> getKeysForSynchronization() {
      return Collections.singleton(1);
    }

  };

  private static final ExecutionStrategy<CalculatorReq> EXECUTION = new ExecutionStrategy<CalculatorReq>() {
    @Override
    public Location getExecutionLocation(CalculatorReq m) {
      return Location.BOTH;
    }
  };


  @Override
  public long getVersion() {
    return 1L;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return Calculator.class.getName().equals(typeName);
  }

  @Override
  public ActiveServerEntity<CalculatorReq, CalculatorRsp> createActiveEntity(ServiceRegistry registry, byte[] configuration) {
    return new CalculatorActiveServerEntity();
  }

  @Override
  public PassiveServerEntity<CalculatorReq, CalculatorRsp> createPassiveEntity(ServiceRegistry registry, byte[] configuration) {
    throw new UnsupportedOperationException("No passive support");
  }

  @Override
  public ConcurrencyStrategy<CalculatorReq> getConcurrencyStrategy(byte[] configuration) {
    return CONCURRENCY;
  }

  @Override
  public ExecutionStrategy<CalculatorReq> getExecutionStrategy(byte[] configuration) {
    return EXECUTION;
  }

  @Override
  public MessageCodec<CalculatorReq, CalculatorRsp> getMessageCodec() {
    return new CalculatorCodec();
  }

  @Override
  public SyncMessageCodec<CalculatorReq> getSyncMessageCodec() {
    return new SyncMessageCodec<CalculatorReq>() {
      @Override
      public byte[] encode(int concurrencyKey, CalculatorReq response) throws MessageCodecException {
        return new byte[0];
      }

      @Override
      public CalculatorReq decode(int concurrencyKey, byte[] payload) throws MessageCodecException {
        return null;
      }
    };
  }


}
