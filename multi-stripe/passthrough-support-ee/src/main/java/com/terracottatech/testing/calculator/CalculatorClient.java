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

import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

/**
 *
 */
public class CalculatorClient implements Calculator {

  private final EntityClientEndpoint<CalculatorReq, CalculatorRsp> endpoint;

  public CalculatorClient(EntityClientEndpoint<CalculatorReq, CalculatorRsp> endpoint) {
    this.endpoint = endpoint;
    this.endpoint.setDelegate(new EndpointDelegate<CalculatorRsp>() {
      @Override
      public void handleMessage(CalculatorRsp messageFromServer) {

      }

      @Override
      public byte[] createExtendedReconnectData() {
        return new byte[0];
      }

      @Override
      public void didDisconnectUnexpectedly() {

      }
    });
  }

  @Override
  public int addition(int x, int y) {
    try {
      CalculatorRsp calculatorRsp = endpoint.beginInvoke().message(new CalculatorReq(x, y, CalculatorReq.Operation.ADDITION)).invoke().get();
      return calculatorRsp.getResult();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (EntityException e) {
      throw new RuntimeException(e);
    } catch (MessageCodecException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    endpoint.close();
  }
}
