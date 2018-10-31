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

package com.terracottatech.store.client.stream;

import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;

import static org.junit.Assert.assertNotNull;

/**
 * Verifies closure semantics for the Java {@link DoubleStream} implementation.
 */
public class JavaDoubleStreamClosureTest extends AbstractDoubleStreamClosureTest {
  @Override
  protected DoubleStream getStream() {
    return DoubleStream.of(1.0D, 2.0D);
  }

  @Override
  public void testBaseStreamOnClose() throws Exception {
    BaseStream<?, ?> stream = close(getStream());
    if (javaMajor() >= 9) {
      assertThrows(() -> stream.onClose(() -> { }), IllegalStateException.class);
    } else {
      assertNotNull(stream.onClose(() -> {}));
    }
  }
}
