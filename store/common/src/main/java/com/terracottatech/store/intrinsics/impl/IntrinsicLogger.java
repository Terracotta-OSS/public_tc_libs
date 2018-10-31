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

import com.terracottatech.store.intrinsics.IntrinsicFunction;
import com.terracottatech.store.intrinsics.IntrinsicType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public class IntrinsicLogger<T> extends LeafIntrinsic implements Consumer<T> {

  private final String message;
  private final List<IntrinsicFunction<? super T, ?>> mappers;
  private static final Logger logger = LoggerFactory.getLogger("StreamLogger");
  private String streamIdentifier;

  public IntrinsicLogger(String logMessageFormat, List<IntrinsicFunction<? super T, ?>> mappers) {
    super(IntrinsicType.CONSUMER_LOG);
    this.message = logMessageFormat;
    this.mappers = mappers;
  }

  public String getMessage() {
    return message;
  }

  public List<IntrinsicFunction<? super T, ?>> getMappers() {
    return mappers;
  }

  public void setStreamIdentifier(String streamIdentifier) {
    this.streamIdentifier = streamIdentifier;
  }

  @Override
  public void accept(T t) {
    String args[] = new String[mappers.size()];

    int i = 0;
    for (Function<? super T, ?> delegate : mappers) {
      args[i++] = String.valueOf(delegate.apply(t));
    }

    if (streamIdentifier != null) {
      logger.info(streamIdentifier + message, (Object[]) args);
    } else {
      logger.info(message, (Object[]) args);
    }
  }

  @Override
  public String toString() {
    return "log(\"" + message + "\"," + mappers.stream().map(Object::toString).collect(joining(",")) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IntrinsicLogger<?> that = (IntrinsicLogger<?>) o;
    return Objects.equals(message, that.message) &&
            Objects.equals(mappers, that.mappers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), message, mappers);
  }
}