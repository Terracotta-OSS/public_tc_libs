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

import com.terracottatech.sovereign.impl.memory.KeySlotShardEngine;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.crud.KeyMessage;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class MutationConcurrencyStrategyTest {
  @Test
  public void concurrencyKey() throws Exception {
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(Type.LONG, Optional.of(4));
    MutationConcurrencyStrategy<Integer> mutationConcurrencyStrategy = new MutationConcurrencyStrategy<>(concurrencyShardMapper);
    Set<Integer> syncKeys = new HashSet<>(Arrays.asList(1,2,3,4));

    Random random = new Random();
    KeySlotShardEngine keySlotShardEngine = new KeySlotShardEngine(4);
    random.ints(1000).forEach(x -> {
      int key = mutationConcurrencyStrategy.concurrencyKey(new TestMessage<>(x));
      int shardIndexForKey = keySlotShardEngine.shardIndexForKey(x);
      assertThat(shardIndexForKey, equalTo(key - 1));
      assertThat(syncKeys.contains(key), is(true));
    });

  }

  @Test
  public void getKeysForSynchronization() throws Exception {
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(Type.LONG, Optional.of(4));
    MutationConcurrencyStrategy<?> mutationConcurrencyStrategy = new MutationConcurrencyStrategy<>(concurrencyShardMapper);
    Set<?> keysForSynchronization = mutationConcurrencyStrategy.getKeysForSynchronization();
    assertThat(keysForSynchronization.size(), is(5));
    AtomicInteger counter = new AtomicInteger(1);
    keysForSynchronization.forEach(x -> assertThat(x, is(counter.getAndIncrement())));
  }

  @Test
  public void testConcurrencyStrategyWhenConcurrencyIsOne() {
    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(Type.LONG, Optional.of(1));
    MutationConcurrencyStrategy<Boolean> mutationConcurrencyStrategy = new MutationConcurrencyStrategy<>(concurrencyShardMapper);

    Random random = new Random();
    KeySlotShardEngine keySlotShardEngine = new KeySlotShardEngine(1);
    random.ints(1000).forEach(x -> {
      boolean bVal = x % 2 == 0;
      int key = mutationConcurrencyStrategy.concurrencyKey(new TestMessage<>(bVal));
      int shardIndexForKey = keySlotShardEngine.shardIndexForKey(bVal);
      assertThat(shardIndexForKey, equalTo((key) - 1));
      assertThat(key, is(1));
    });
  }

  private static class TestMessage<K extends Comparable<K>> extends KeyMessage<K> {

    public TestMessage(K key) {
      super(key);
    }

    @Override
    public DatasetOperationMessageType getType() {
      return null;
    }
  }

}