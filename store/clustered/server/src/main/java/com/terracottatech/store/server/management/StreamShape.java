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
package com.terracottatech.store.server.management;

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.impl.Constant;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;

public class StreamShape {

  private final String shape;
  private volatile Boolean indexed;

  private StreamShape(String shape) {
    this.shape = requireNonNull(shape);
  }

  public static StreamShape shapeOf(List<PipelineOperation> intermediates, PipelineOperation terminal) {
    Stream<PipelineOperation> operations;
    if (terminal == null) {
      operations = intermediates.stream();
    } else {
      operations = concat(intermediates.stream(), of(terminal));
    }
    return new StreamShape(concat(of("records()"),
        operations.map(op -> op.getArguments().stream()
            .map(StreamShape::translateArgument)
            .collect(joining(", ", translateOperation(op.getOperation()) + "(", ")"))))
        .collect(joining(".")));
  }

  private static String translateArgument(Object argument) {
    if (argument instanceof Intrinsic) {
      return ((Intrinsic) argument).toString(i -> {
        if (i instanceof Constant<?, ?>) {
          return "?";
        } else {
          return i.toString();
        }
      });
    } else {
      return argument.toString();
    }
  }

  private static final Pattern CAMELIZE = Pattern.compile("(^|_)([A-Z])([A-Z]+)");
  private static CharSequence translateOperation(PipelineOperation.Operation operation) {
    Matcher matcher = CAMELIZE.matcher(operation.name());

    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      if (matcher.group(1).isEmpty()) {
        sb.append(matcher.group(2).toLowerCase(Locale.ROOT));
      } else {
        sb.append(matcher.group(2));
      }
      sb.append(matcher.group(3).toLowerCase(Locale.ROOT));
    }
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return shape.hashCode() ^ Objects.hashCode(indexed);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StreamShape) {
      StreamShape other = (StreamShape) obj;
      return shape.equals(other.shape) && Objects.equals(indexed, other.indexed);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return shape.toString() + (indexed == null ? " [indexing-unknown]" : indexed ? " [indexed]" : " [not-indexed]");
  }

  public void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }
}
