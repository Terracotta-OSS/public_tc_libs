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

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import org.terracotta.entity.ConcurrencyStrategy;

import java.util.Set;

public abstract class HashCodeConcurrencyStrategy<M extends DatasetEntityMessage> implements ConcurrencyStrategy<M> {
  private static Set<Integer> ALL_KEYS = RangeHelper.rangeClosed(1, 65536);

  protected int hashCode(Object key) {
    int hashCode = key.hashCode();

    int lowBits = (hashCode << 16) >>> 16;
    int shiftedHighBits = hashCode >>> 16;

    return (lowBits ^ shiftedHighBits) + 1;
  }

  @Override
  public Set<Integer> getKeysForSynchronization() {
    return ALL_KEYS;
  }
}
