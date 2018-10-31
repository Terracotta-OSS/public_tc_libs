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

package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.intrinsics.IntrinsicCollector;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.function.BiConsumer;
import java.util.stream.Collector;

public class FilteringCollector<T, A, R>
        extends CascadingCollector<T, A, R, IntrinsicPredicate<? super T>, IntrinsicCollector<T, A, R>> {

  public FilteringCollector(IntrinsicPredicate<? super T> predicate, IntrinsicCollector<T, A, R> downstream) {
    super(IntrinsicType.COLLECTOR_FILTERING, predicate, downstream, FilteringCollector::filtering, "filtering");
  }

  /**
   * To be replaced with Collectors.filtering in Java 9+.
   */
  private static <T, A, R> Collector<T, A, R> filtering(
          IntrinsicPredicate<? super T> predicate, IntrinsicCollector<T, A, R> downstream) {
    BiConsumer<A, T> accumulator = downstream.accumulator();
    return Collector.of(
            downstream.supplier(),
            (state, t) -> {
              if (predicate.test(t))
                accumulator.accept(state, t);
            },
            downstream.combiner(),
            downstream.finisher(),
            downstream.characteristics().toArray(new Collector.Characteristics[0]));
  }
}
