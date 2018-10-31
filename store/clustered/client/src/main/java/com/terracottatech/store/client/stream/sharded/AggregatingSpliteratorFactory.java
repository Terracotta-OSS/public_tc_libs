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

package com.terracottatech.store.client.stream.sharded;

import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

abstract class AggregatingSpliteratorFactory<T, SPLIT extends Spliterator<T>> implements Supplier<SPLIT> {

  private final Stream.Builder<Supplier<? extends SPLIT>> spliterators = Stream.builder();
  private final int characteristics;
  private final boolean parallel;

  AggregatingSpliteratorFactory(Supplier<? extends SPLIT> a, Supplier<? extends SPLIT> b, int characteristics, boolean parallel) {
    Stream.of(a, b).forEach(s -> {
      if (s instanceof AggregatingSpliteratorFactory) {
        @SuppressWarnings("unchecked")
        AggregatingSpliteratorFactory<T, ? extends SPLIT> factory = (AggregatingSpliteratorFactory<T, ? extends SPLIT>) s;
        factory.spliterators.build().forEach(spliterators::accept);
      } else {
        Stream.of(s).forEach(spliterators::accept);
      }
    });
    this.characteristics = characteristics;
    this.parallel = parallel;
  }

  @Override
  public SPLIT get() {
    if (parallel) {
      return get(spliterators.build().parallel(), characteristics);
    } else {
      return get(spliterators.build(), characteristics);
    }
  }

  abstract SPLIT get(Stream<Supplier<? extends SPLIT>> supplierStream, int characteristics);
}
