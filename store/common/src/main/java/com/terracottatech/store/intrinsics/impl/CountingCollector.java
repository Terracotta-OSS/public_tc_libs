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

import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicCollector;
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Implements a portable version of {@link java.util.stream.Collectors#counting()}.
 */
public class CountingCollector<T> implements IntrinsicCollector<T, Object, Long> {

  private static final IntrinsicCollector<?, ?, Long> INSTANCE = new CountingCollector<>();

  @SuppressWarnings("unchecked")
  public static <T> IntrinsicCollector<T, ?, Long> counting() {
    return (IntrinsicCollector<T, ?, Long>)INSTANCE;
  }

  @SuppressWarnings("unchecked")
  private final Collector<T, Object, Long> realCountingCollector = (Collector<T, Object, Long>)Collectors.counting();

  private CountingCollector() {
  }

  @Override
  public IntrinsicType getIntrinsicType() {
    return IntrinsicType.COUNTING_COLLECTOR;
  }

  @Override
  public List<Intrinsic> incoming() {
    return Collections.emptyList();
  }

  @Override
  public Supplier<Object> supplier() {
    return realCountingCollector.supplier();
  }

  @Override
  public BiConsumer<Object, T> accumulator() {
    return realCountingCollector.accumulator();
  }

  @Override
  public BinaryOperator<Object> combiner() {
    return realCountingCollector.combiner();
  }

  @Override
  public Function<Object, Long> finisher() {
    return realCountingCollector.finisher();
  }

  @Override
  public Set<Characteristics> characteristics() {
    return realCountingCollector.characteristics();
  }

  @Override
  public String toString(Function<Intrinsic, String> formatter) {
    return toString();
  }

  @Override
  public String toString() {
    return "counting()";
  }
}
