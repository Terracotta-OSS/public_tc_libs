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
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collector;

abstract class AbstractGroupingCollector<T, K, A, D, R extends Map<K, D>>
        extends CascadingCollector<T, Object, R, IntrinsicFunction<? super T, K>, IntrinsicCollector<? super T, A, D>> {

  AbstractGroupingCollector(IntrinsicType intrinsicType, IntrinsicFunction<? super T, K> classifier,
                            IntrinsicCollector<? super T, A, D> downstream,
                            BiFunction<IntrinsicFunction<? super T, K>, IntrinsicCollector<? super T, A, D>, Collector<T, ?, R>> delegator,
                            String name) {

    super(intrinsicType, classifier, downstream, delegator, name);
  }
}
