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

import com.terracottatech.store.common.messages.crud.KeyMessage;

import org.terracotta.entity.ConcurrencyStrategy;

import java.util.Set;

public class MutationConcurrencyStrategy<K extends Comparable<K>> implements ConcurrencyStrategy<KeyMessage<K>> {

  private final Set<Integer> rangeOfKeys;
  private final ConcurrencyShardMapper concurrencyShardMapper;

  public MutationConcurrencyStrategy(ConcurrencyShardMapper concurrencyShardMapper) {
    this.concurrencyShardMapper = concurrencyShardMapper;
    this.rangeOfKeys = RangeHelper.rangeClosed(1, concurrencyShardMapper.getShardCount() + 1);   //the additional key to sync message tracker data for indexes and streams
  }

  @Override
  public int concurrencyKey(KeyMessage<K> keyMessage) {
    return concurrencyShardMapper.getConcurrencyKey(keyMessage.getKey());
  }

  @Override
  public Set<Integer> getKeysForSynchronization() {
    return rangeOfKeys;
  }
}
