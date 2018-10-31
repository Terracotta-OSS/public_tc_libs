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
package com.terracottatech.store.client.stream;

import java.util.Spliterator;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * A delegating {@link Spliterator.OfPrimitive} that closes the associated {@link Stream} once the spliterator
 * is exhausted.  Subclasses of this class <b>must</b> implement one of the subclasses of
 * {@code Spliterator.OfPrimitive}.
 */
@SuppressWarnings("try")
abstract class PrimitiveAutoClosingSpliterator<T, T_CONS,
      T_SPLIT extends Spliterator.OfPrimitive<T, T_CONS, T_SPLIT>,
      R_SPLIT extends Spliterator.OfPrimitive<T, T_CONS, T_SPLIT> & RemoteSpliterator<T>>
    extends AbstractAutoClosingSpliterator<T, R_SPLIT, T_SPLIT>
    implements Spliterator.OfPrimitive<T, T_CONS, T_SPLIT>, AutoCloseable {

  protected PrimitiveAutoClosingSpliterator(R_SPLIT delegate, BaseStream<?, ?> headStream) {
    super(delegate, headStream);
  }

  protected PrimitiveAutoClosingSpliterator(PrimitiveAutoClosingSpliterator<T, T_CONS, T_SPLIT, R_SPLIT> owner, T_SPLIT split) {
    super(owner, split);
  }

  @Override
  public final boolean tryAdvance(T_CONS action) {
    return tryAdvanceInternal(() -> delegate.tryAdvance(action));
  }

  @Override
  public final void forEachRemaining(T_CONS action) {
    forEachRemainingInternal(() -> delegate.forEachRemaining(action));
  }
}
