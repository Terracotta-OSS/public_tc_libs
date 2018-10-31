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

import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.PassiveSynchronizationChannel;

/**
 *
 */
public class CalculatorActiveServerEntity implements ActiveServerEntity<CalculatorReq, CalculatorRsp> {


  public CalculatorActiveServerEntity() {
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {

  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
  }

  @Override
  public CalculatorRsp invokeActive(ActiveInvokeContext<CalculatorRsp> context, CalculatorReq message) {
    switch (message.getOperation()) {
      case ADDITION:
        int x = message.getX();
        int y = message.getY();
        return new CalculatorRsp(x + y);
      default:
        throw new UnsupportedOperationException("No support for operation : " + message.getOperation());
    }
  }

  @Override
  public ReconnectHandler startReconnect() {
    return (clientDescriptor, bytes) -> {

    };
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<CalculatorReq> syncChannel, int concurrencyKey) {

  }

  @Override
  public void createNew() {

  }

  @Override
  public void loadExisting() {

  }

  @Override
  public void destroy() {

  }

}
