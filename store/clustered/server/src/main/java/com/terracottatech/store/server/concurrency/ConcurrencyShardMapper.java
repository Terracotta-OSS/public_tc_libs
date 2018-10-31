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

import com.terracottatech.sovereign.impl.ConcurrencyFormulator;
import com.terracottatech.sovereign.impl.memory.KeySlotShardEngine;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.DatasetEntityConfiguration;

import java.util.Optional;

public class ConcurrencyShardMapper {

  private final KeySlotShardEngine shardEngine;

  public ConcurrencyShardMapper(DatasetEntityConfiguration<?> configuration) {
    this(configuration.getKeyType(), configuration.getDatasetConfiguration().getConcurrencyHint());
  }

  public <K extends Comparable<K>> ConcurrencyShardMapper(Type<K> keyType, Optional<Integer> concurrency) {
    ConcurrencyFormulator<K> concurrencyFormulator = new ConcurrencyFormulator<>(keyType.getJDKType(), concurrency);
    this.shardEngine = new KeySlotShardEngine(concurrencyFormulator.getConcurrency());
  }

  public int getShardCount() {
    return shardEngine.getShardCount();
  }

  int getConcurrencyKey(Object key) {
    return shardIndexToConcurrencyKey(getShardIndex(key));
  }

  int getConcurrencyKey(long index) {
    return shardIndexToConcurrencyKey(getShardIndex(index));
  }

  public int getShardIndex(Object key) {
    return shardEngine.shardIndexForKey(key);
  }

  public int getShardIndex(long index) {
    return shardEngine.extractShardIndexLambda().transmute(index);
  }

  static int shardIndexToConcurrencyKey(int shardIndex) {
    return shardIndex + 1;
  }

  public static int concurrencyKeyToShardIndex(int concurrencyKey) {
    return concurrencyKey - 1;
  }
}
