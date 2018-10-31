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
import java.util.Spliterator.OfInt;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SuppressWarnings("try")
final class SequencedSpliteratorOfInt extends SequencedSpliterator<Integer, OfInt> implements OfInt {

  private SequencedSpliteratorOfInt(Stream<Supplier<? extends OfInt>> spliterators, int characteristics) {
    super(spliterators, characteristics);
  }

  private SequencedSpliteratorOfInt(Spliterator<Supplier<? extends OfInt>> spliterators, int characteristics) {
    super(spliterators, characteristics);
  }

  @Override
  public boolean tryAdvance(IntConsumer action) {
    return super.tryAdvance(action::accept);
  }

  @Override
  public void forEachRemaining(IntConsumer action) {
    super.forEachRemaining(action::accept);
  }

  @Override
  public OfInt trySplit() {
    Spliterator<Supplier<? extends OfInt>> spliterator = spliterators.trySplit();
    if (spliterator == null) {
      return null;
    } else {
      return new SequencedSpliteratorOfInt(spliterator, characteristics());
    }
  }

  static class FactoryOfInt extends AggregatingSpliteratorFactory<Integer, OfInt> {

    FactoryOfInt(Supplier<? extends OfInt> a, Supplier<? extends OfInt> b, int characteristics, boolean parallel) {
      super(a, b, characteristics, parallel);
    }

    @Override
    OfInt get(Stream<Supplier<? extends OfInt>> supplierStream, int characteristics) {
      return new SequencedSpliteratorOfInt(supplierStream, characteristics);
    }
  }
}
