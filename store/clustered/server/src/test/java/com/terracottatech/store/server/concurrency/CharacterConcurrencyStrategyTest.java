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

import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CharacterConcurrencyStrategyTest {
  private final UUID stableClientId = UUID.randomUUID();

  @Test
  public void asciiPlusOne() {
    CharacterConcurrencyStrategy strategy = new CharacterConcurrencyStrategy();
    assertEquals(66, strategy.concurrencyKey(new PredicatedDeleteRecordMessage<>(stableClientId, 'A', null, false)));
  }

  @Test
  public void zeroChar() {
    CharacterConcurrencyStrategy strategy = new CharacterConcurrencyStrategy();
    assertEquals(1, strategy.concurrencyKey(new PredicatedDeleteRecordMessage<>(stableClientId, (char) 0, null, false)));
  }

  @Test
  public void maxChar() {
    CharacterConcurrencyStrategy strategy = new CharacterConcurrencyStrategy();
    assertEquals(65_536, strategy.concurrencyKey(new PredicatedDeleteRecordMessage<>(stableClientId, Character.MAX_VALUE, null, false)));
  }

  @Test
  public void syncKeys() {
    CharacterConcurrencyStrategy strategy = new CharacterConcurrencyStrategy();
    Set<Integer> keys = strategy.getKeysForSynchronization();

    assertEquals(65_536, keys.size());
    assertTrue(keys.contains(1));
    assertTrue(keys.contains(65_536));
  }
}
