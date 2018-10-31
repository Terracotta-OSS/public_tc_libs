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
package com.terracottatech.store.server.concurrency;

import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordMessage;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KeyHashCodeConcurrencyStrategyTest {
  private final UUID stableClientId = UUID.randomUUID();

  @Test
  public void zero() {
    runSingleValueTest(0);
  }

  @Test
  public void one() {
    runSingleValueTest(1);
  }

  @Test
  public void minusOne() {
    runSingleValueTest(-1);
  }

  @Test
  public void minValue() {
    runSingleValueTest(Integer.MIN_VALUE);
  }

  @Test
  public void maxValue() {
    runSingleValueTest(Integer.MAX_VALUE);
  }

  private void runSingleValueTest(int value) {
    KeyHashCodeConcurrencyStrategy<Integer> strategy = new KeyHashCodeConcurrencyStrategy<>();
    Set<Integer> keysForSynchronization = strategy.getKeysForSynchronization();

    int concurrencyKey = strategy.concurrencyKey(new PredicatedDeleteRecordMessage<>(stableClientId, value, null, false));
    assertTrue(keysForSynchronization.contains(concurrencyKey));
  }

  @Test
  public void distinctTest() {
    KeyHashCodeConcurrencyStrategy<Integer> strategy = new KeyHashCodeConcurrencyStrategy<>();

    Set<Integer> keysSoFar = new HashSet<>();

    for (int i = 0; i < 65536; i++) {
      int concurrencyKey = strategy.concurrencyKey(new PredicatedDeleteRecordMessage<>(stableClientId, i, null, false));
      assertTrue(keysSoFar.add(concurrencyKey));
    }
  }

  @Test
  public void rangeTest() {
    KeyHashCodeConcurrencyStrategy<Integer> strategy = new KeyHashCodeConcurrencyStrategy<>();
    Set<Integer> keysForSynchronization = strategy.getKeysForSynchronization();

    Random random = new Random(1L);

    for (int i = 0; i < 1_000_000; i++) {
      int concurrencyKey = strategy.concurrencyKey(new PredicatedDeleteRecordMessage<>(stableClientId, random.nextInt(), null, false));
      if (!keysForSynchronization.contains(concurrencyKey)) {
        fail("Bad concurrency key for: " + i);
      }
    }
  }
}
