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
package com.terracottatech.sovereign.impl;

import com.terracottatech.store.Type;

import java.io.Serializable;
import java.util.Optional;

@SuppressWarnings("serial")
public class ConcurrencyFormulator<K extends Comparable<K>> implements Serializable {

  private final Class<K> keyType;
  private final int concurrency;

  public ConcurrencyFormulator(Class<K> keyType, Optional<Integer> concurrency) {
    this.keyType = keyType;
    if (Type.forJdkType(keyType).equals(Type.BOOL)) {
      this.concurrency = 1;
    } else {
      this.concurrency = concurrency.map(this::powerOf2).orElseGet(ConcurrencyFormulator::defaultConcurrency);
    }
  }

  public int getConcurrency() {
    return this.concurrency;
  }

  // power of two default concurrency
  private static int defaultConcurrency() {
    int procs = Runtime.getRuntime().availableProcessors();
    int i = 1;
    while (i < procs) {
      i = i << 1;
    }
    return Math.min(i, 8);
  }

  private int powerOf2(int conc) {
    if (Integer.highestOneBit(conc) != conc) {
      conc = Integer.highestOneBit(conc) << 1;
    }
    return conc;
  }

}
