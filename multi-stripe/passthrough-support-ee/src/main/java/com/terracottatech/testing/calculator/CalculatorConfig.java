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

import java.util.Objects;

/**
 * @author Ludovic Orban
 */
public class CalculatorConfig {

  private final String brand;

  public CalculatorConfig(String brand) {
    this.brand = brand;
  }

  public String getBrand() {
    return brand;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(brand);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof CalculatorConfig) {
      CalculatorConfig other = (CalculatorConfig) obj;
      return Objects.equals(brand, other.brand);
    }
    return false;
  }
}
