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

import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

public class DefaultGroupingCollector<T, K, A, D> extends AbstractGroupingCollector<T, K, A, D, Map<K, D>> {

  public DefaultGroupingCollector(IntrinsicFunction<? super T, K> classifier,
                                  IntrinsicCollector<? super T, A, D> downstream) {
    super(IntrinsicType.COLLECTOR_GROUPING, classifier, downstream,
            (c, d) -> groupingBy(c, HashMap::new, d), "groupingBy");
  }
}
