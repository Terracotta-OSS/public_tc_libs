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

import java.util.Comparator;
import java.util.Spliterator.OfLong;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SuppressWarnings("try")
final class SortedSpliteratorOfLong extends SortedSpliterator<Long> implements OfLong {

  private SortedSpliteratorOfLong(Stream<Supplier<? extends OfLong>> spliterators, Comparator<? super Long> comparator, int characteristics) {
    super(spliterators, comparator, characteristics);
  }

  @Override
  public boolean tryAdvance(LongConsumer action) {
    return super.tryAdvance(action::accept);
  }

  @Override
  public OfLong trySplit() {
    return (OfLong) super.trySplit();
  }

  public static class FactoryOfLong extends Factory<Long, OfLong> {

    FactoryOfLong(Supplier<? extends OfLong> a, Supplier<? extends OfLong> b, Comparator<? super Long> comparator, int characteristics) {
      super(a, b, comparator, characteristics);
    }

    @Override
    public OfLong get(Stream<Supplier<? extends OfLong>> supplierStream, Comparator<? super Long> comparator, int characteristics) {
      return new SortedSpliteratorOfLong(supplierStream, comparator, characteristics);
    }
  }
}
