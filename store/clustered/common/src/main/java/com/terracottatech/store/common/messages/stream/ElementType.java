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

package com.terracottatech.store.common.messages.stream;

import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Identifies a remote stream element type.
 */
public enum ElementType {
  /**
   * The element is a {@link com.terracottatech.store.Record Record}.
   */
  RECORD {
    @Override
    public Element from(Object data) {
      throw new UnsupportedOperationException("Must encode RECORD message separately");
    }

    @Override
    public <T, S extends BaseStream<T, S>> boolean validateStreamType(S stream) {
      return (stream instanceof Stream<?>);
    }
  },

  /**
   * The element is a {@code double} (feeding a {@link java.util.Spliterator.OfDouble Spliterator.OfDouble}).
   */
  DOUBLE {
    @Override
    public <T, S extends BaseStream<T, S>> boolean validateStreamType(S stream) {
      return (stream instanceof DoubleStream);
    }
  },

  /**
   * The element is a {@code int} (feeding a {@link java.util.Spliterator.OfInt Spliterator.OfInt}).
   */
  INT {
    @Override
    public <T, S extends BaseStream<T, S>> boolean validateStreamType(S stream) {
      return (stream instanceof IntStream);
    }
  },

  /**
   * The element is a {@code long} (feeding a {@link java.util.Spliterator.OfLong Spliterator.OfLong}).
   */
  LONG {
    @Override
    public <T, S extends BaseStream<T, S>> boolean validateStreamType(S stream) {
      return (stream instanceof LongStream);
    }
  },

  /**
   * The element is a supported {@link Stream} element.  See {@link ElementValue} for
   * the supported types.
   */
  ELEMENT_VALUE {
    @Override
    public Element from(Object data) {
      return new Element(this, ElementValue.ValueType.forObject(data).createElementValue(data));
    }

    @Override
    public <T, S extends BaseStream<T, S>> boolean validateStreamType(S stream) {
      return (stream instanceof Stream);
    }
  },

  TERMINAL {
    @Override
    public Element from(Object data) {
      throw new UnsupportedOperationException("Encoding not supported for TERMINAL");
    }

    @Override
    public <T, S extends BaseStream<T, S>> boolean validateStreamType(S stream) {
      return true;
    }
  },
  ;

  public Element from(Object data) {
    return new Element(this, data);
  }

  /**
   * Verifies the specified {@code Stream} is of the type required to carry the element type.
   * @param stream the {@code BaseStream} to check
   * @param <T> the element type of the stream
   * @param <S> the stream implementation type
   * @return {@code true} if {@code stream} appropriate for this element type; {@code false} otherwise
   */
  public abstract <T, S extends BaseStream<T, S>> boolean validateStreamType(S stream);
}
