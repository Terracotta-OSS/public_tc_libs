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
package com.terracottatech.store.server.stream;

/**
 * A {@link InlineElementSource#fetch()} response when the response is <b>not</b> a stream element.
 *
 * @see #EXHAUSTED
 */
public abstract class NonElement {
  /**
   * A {@code NonElement} indicating the stream is exhausted.
   */
  public static final NonElement EXHAUSTED = new NonElement() {
    @Override
    public String toString() {
      return "NonElement.EXHAUSTED{}";
    }
  };

  /**
   * A {@code NonElement} indicating the stream element was consumed by the pipeline -- no
   * element to return.
   */
  public static final NonElement CONSUMED = new NonElement() {
    @Override
    public String toString() {
      return "NonElement.CONSUMED{}";
    }
  };

  /**
   * A {@code NonElement} indicating the stream element was dropped by the client-side pipeline --
   * server-side processing must be discontinued.
   */
  public static final NonElement RELEASED = new NonElement() {
    @Override
    public String toString() {
      return "NonElement.RELEASED{}";
    }
  };
}
