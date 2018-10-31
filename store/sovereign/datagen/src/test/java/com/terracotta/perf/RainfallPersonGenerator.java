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

package com.terracotta.perf;

import io.rainfall.ObjectGenerator;

import com.terracotta.perf.data.PersonGenerator;

import java.util.Random;

/**
 * Sample class to confirm Rainfall generator implementation.
 *
 * @author Clifford W. Johnson
 *
 * @see com.terracotta.perf.data.PersonGenerator
 */
public class RainfallPersonGenerator extends PersonGenerator<Person> implements ObjectGenerator<Person> {
  public static final long ID_FACTOR = 500000000;

  public RainfallPersonGenerator(final int size) {
    super(new com.terracotta.perf.PersonBuilder(true), size);
    this.setBirthDatePercentage(100.0F);    // Change from default of 90.0%
    this.setGenderPercentage(100.0F);       // Change from default of 95.0%
    this.setLocationPercentage(0.0F);       // Suppress location generation
  }

  @Override
  public Person generate(Long seed) {
    return generate(new Random(seed), ID_FACTOR + seed);
  }

  @Override
  public String getDescription() {
    return "PersonGenerator";
  }
}
