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

import com.terracottatech.store.intrinsics.IntrinsicToIntFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.stream.Collector;
import java.util.stream.Collectors;

public class SummarizingIntCollector<T, R> extends LeafIntrinsicCollector<T, Object, R> {

  private final IntrinsicToIntFunction<? super T> mapper;

  @SuppressWarnings("unchecked")
  public SummarizingIntCollector(IntrinsicToIntFunction<? super T> mapper) {
    super(IntrinsicType.COLLECTOR_SUMMARIZING_INT, (Collector<T, Object, R>)Collectors.<T>summarizingInt(mapper));
    this.mapper = mapper;
  }

  public IntrinsicToIntFunction<? super T> getMapper() {
    return mapper;
  }

  @Override
  public String toString() {
    return "summarizingInt(" + mapper + ")";
  }
}
