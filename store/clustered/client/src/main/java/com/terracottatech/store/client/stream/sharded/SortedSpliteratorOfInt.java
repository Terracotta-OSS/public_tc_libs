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
import java.util.Spliterator.OfInt;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SuppressWarnings("try")
final class SortedSpliteratorOfInt extends SortedSpliterator<Integer> implements OfInt {

  private SortedSpliteratorOfInt(Stream<Supplier<? extends OfInt>> spliterators, Comparator<? super Integer> comparator, int characteristics) {
    super(spliterators, comparator, characteristics);
  }

  @Override
  public boolean tryAdvance(IntConsumer action) {
    return super.tryAdvance(action::accept);
  }

  @Override
  public OfInt trySplit() {
    return (OfInt) super.trySplit();
  }

  static class FactoryOfInt extends Factory<Integer, OfInt> {

    FactoryOfInt(Supplier<? extends OfInt> a, Supplier<? extends OfInt> b, Comparator<? super Integer> comparator, int characteristics) {
      super(a, b, comparator, characteristics);
    }

    @Override
    public OfInt get(Stream<Supplier<? extends OfInt>> supplierStream, Comparator<? super Integer> comparator, int characteristics) {
      return new SortedSpliteratorOfInt(supplierStream, comparator, characteristics);
    }
  }
}
