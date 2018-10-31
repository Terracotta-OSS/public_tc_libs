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

import com.terracottatech.entity.AggregateEndpoint;
import com.terracottatech.entity.EntityAggregatingService;

/**
 * @author Ludovic Orban
 */
public class CalculatorEntityAggregatingService implements EntityAggregatingService<Calculator, CalculatorConfig> {
  @Override
  public boolean handlesEntityType(Class<Calculator> cls) {
    return Calculator.class.equals(cls);
  }

  @Override
  public Calculator aggregateEntities(AggregateEndpoint<Calculator> endpoint) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean targetConnectionForLifecycle(int stripeIndex, int totalStripes, String entityName, CalculatorConfig config) {
    return false;
  }

  @Override
  public CalculatorConfig formulateConfigurationForStripe(int stripeIndex, int totalStripes, String entityName, CalculatorConfig config) {
    return config;
  }

  @Override
  public byte[] serializeConfiguration(CalculatorConfig configuration) {
    return CalculatorCodec.serializeConfiguration(configuration);
  }

  @Override
  public CalculatorConfig deserializeConfiguration(byte[] configuration) {
    return CalculatorCodec.deserializeConfiguration(configuration);
  }
}
