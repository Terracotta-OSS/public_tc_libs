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

import org.junit.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import static java.lang.System.lineSeparator;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Verifies closure semantics for the Java {@link Stream} implementation.
 */
public class JavaStreamClosureTest extends AbstractStreamClosureTest<String> {

  @Override
  protected Stream<String> getStream() {
    return Stream.of("one", "two");
  }

  @Test
  public void testConsumptionDoesNotAutoClose() {
    Stream<String> stream = getStream();

    Runnable closed = mock(Runnable.class);
    Stream<String> derived = stream.onClose(closed);
    assertThat(derived, sameInstance(stream));

    assertThat(stream.reduce(String::concat), is(Optional.of("onetwo")));
    verify(closed, never()).run();

    stream.close();
    verify(closed).run();
  }

  @Override
  public void testBaseStreamOnClose() throws Exception {
    BaseStream<String, ?> stream = close(getStream());
    if (javaMajor() >= 9) {
      assertThrows(() -> stream.onClose(() -> { }), IllegalStateException.class);
    } else {
      assertNotNull(stream.onClose(() -> {}));
    }
  }

  /**
   * This test examines the behavior of {@link BaseStream#spliterator()} under the influence of
   * a source closure.
   */
  @Test
  public void testClosedSource() throws Exception {
    BufferedReader reader = new BufferedReader(new StringReader(
        "First line" + lineSeparator()
        + "Second line" + lineSeparator()
        + "Third line" + lineSeparator()
    ));
    Stream<String> stream = reader.lines();
    Spliterator<String> spliterator = stream.spliterator();

    // Stream closure does not impact spliterator results
    stream.close();
    assertTrue(spliterator.tryAdvance(l -> { }));

    // source closure results in an IOException
    reader.close();
    assertThrows(() -> spliterator.tryAdvance(l -> { }), UncheckedIOException.class);
  }

  /**
   * This test examines the behavior of {@link BaseStream#spliterator()} under the influence of
   * a source exhaustion.
   */
  @Test
  public void testExhaustedSource() throws Exception {
    BufferedReader reader = new BufferedReader(new StringReader(
        "First line" + lineSeparator()
        + "Second line" + lineSeparator()
        + "Third line" + lineSeparator()
    ));
    Stream<String> stream = reader.lines();
    Spliterator<String> spliterator = stream.spliterator();

    // Stream closure does not impact spliterator results
    stream.close();
    assertTrue(spliterator.tryAdvance(l -> { }));

    spliterator.forEachRemaining(l -> { });

    assertFalse(spliterator.tryAdvance(l -> { }));

    // source closure results in an IOException
    reader.close();
    assertThrows(() -> spliterator.tryAdvance(l -> { }), UncheckedIOException.class);
  }
}
