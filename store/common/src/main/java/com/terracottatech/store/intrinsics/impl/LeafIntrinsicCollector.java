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
import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public abstract class LeafIntrinsicCollector<T, A, R> extends LeafIntrinsic implements IntrinsicCollector<T, A, R> {

  private final Supplier<A> supplier;
  private final BiConsumer<A, T> accumulator;
  private final BinaryOperator<A> combiner;
  private final Function<A, R> finisher;
  private final Set<Characteristics> characteristics;

  protected LeafIntrinsicCollector(IntrinsicType type, Supplier<A> supplier, BiConsumer<A, T> accumulator, BinaryOperator<A> combiner, Function<A, R> finisher, Set<Characteristics> characteristics) {
    super(type);
    this.supplier = supplier;
    this.accumulator = accumulator;
    this.combiner = combiner;
    this.finisher = finisher;
    this.characteristics = characteristics;
  }

  protected LeafIntrinsicCollector(IntrinsicType type, Collector<T, A, R> collector) {
    this(type, collector.supplier(), collector.accumulator(), collector.combiner(), collector.finisher(), collector.characteristics());
  }

  @Override
  public Supplier<A> supplier() {
    return supplier;
  }

  @Override
  public BiConsumer<A, T> accumulator() {
    return accumulator;
  }

  @Override
  public BinaryOperator<A> combiner() {
    return combiner;
  }

  @Override
  public Function<A, R> finisher() {
    return finisher;
  }

  @Override
  public Set<Characteristics> characteristics() {
    return characteristics;
  }

}
