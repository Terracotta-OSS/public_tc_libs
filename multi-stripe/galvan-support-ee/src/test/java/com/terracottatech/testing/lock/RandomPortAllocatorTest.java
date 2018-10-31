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
package com.terracottatech.testing.lock;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.security.SecureRandom;
import java.util.Random;

@RunWith(MockitoJUnitRunner.class)
public class RandomPortAllocatorTest {
  @Test
  public void withinRange() {
    int seed = new SecureRandom().nextInt();
    PortAllocator allocator = new RandomPortAllocator(new Random(seed));

    for (int i = 0; i < 10_000; i++) {
      int portBase = allocator.allocatePorts(4);
      if (portBase < 1024 || portBase > 32764) {
        throw new AssertionError("portBase outside range: " + portBase + " seed: " + seed);
      }
    }
  }
}