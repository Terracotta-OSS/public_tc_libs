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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class PartitioningCollector<T, D, A>
        extends CascadingCollector<T, Object, Map<Boolean, D>, IntrinsicPredicate<? super T>, IntrinsicCollector<? super T, A, D>> {

  public PartitioningCollector(
          IntrinsicPredicate<? super T> predicate, IntrinsicCollector<? super T, A, D> downstream) {
    super(IntrinsicType.COLLECTOR_PARTITIONING, predicate, downstream,
            PartitioningCollector::groupingByPredicate, "partitioningBy");
  }

  /**
   * Use {@link Collectors#groupingBy} as it allows to explicitly supply the
   * container as a HashMap (note that any non-concurrent Map is deserialized as a
   * HashMap).
   */
  private static <T, A, D> Collector<T, ?, Map<Boolean, D>> groupingByPredicate(
          Predicate<? super T> predicate, Collector<? super T, A, D> downstream) {
    return groupingBy(predicate::test, HashMap::new, downstream);
  }

  /**
   * The resulting map must contain both true and false keys for
   * consistency with {@link Collectors#partitioningBy}.
   */
  @Override
  public Function<Object, Map<Boolean, D>> finisher() {
    return o -> {
      Map<Boolean, D> result = super.finisher().apply(o);
      result.computeIfAbsent(true, k -> emptyDownstreamResult());
      result.computeIfAbsent(false, k -> emptyDownstreamResult());
      return result;
    };
  }

  private D emptyDownstreamResult() {
    return Stream.<T>empty().collect(getDownstream());
  }
}
