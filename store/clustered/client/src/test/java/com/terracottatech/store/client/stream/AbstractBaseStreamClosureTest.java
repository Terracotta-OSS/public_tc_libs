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

import org.junit.After;
import org.junit.Test;

import java.util.Spliterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.BaseStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests closure semantics for {@link BaseStream} methods.
 */
public abstract class AbstractBaseStreamClosureTest<T, S extends BaseStream<T, S>> {

  /**
   * Post-test encouragement for garbage collection in an attempt to show closure guard faults.
   */
  @After
  public void forceGC() {
    for (int i = 0; i < 5; i++) {
      System.gc();
      System.runFinalization();
    }
  }

   /* ==========================================================================================
    * BaseStream methods
    */

  @Test
  public void testBaseStreamClose() throws Exception {
    S stream = close(getStream());
    stream.close();
  }

  @Test
  public void testBaseStreamIsParallel() throws Exception {
    S stream = close(getStream());
    assertNotNull(stream.isParallel());
  }

  @Test
  public void testBaseStreamParallel() throws Exception {
    S stream = close(getStream());
    assertNotNull(stream.parallel());
  }

  @Test
  public void testBaseStreamSequential() throws Exception {
    S stream = close(getStream());
    assertNotNull(stream.sequential());
  }

  @Test
  public void testBaseStreamUnordered() throws Exception {
    /*
     * The 'unordered' operation becomes a no-op if the stream is already unordered.
     */
    boolean isOrdered;
    try (S testStream = getStream()) {
      isOrdered = testStream.spliterator().hasCharacteristics(Spliterator.ORDERED);
    }
    S stream = close(getStream());
    if (isOrdered) {
      assertThrows(stream::unordered, IllegalStateException.class);
    } else {
      assertNotNull(stream.unordered());
    }
  }

  /**
   * Test the behavior of {@link BaseStream#onClose(Runnable)} when the stream is closed.
   * The behavior between Java 1.8 and Java 9 is different -- Java 1.8 does not check for
   * pipeline termination or closure in {@code onClose} and Java 9 does.  The Java 9
   * behavior is arguably more "proper" - {@code onClose} is documented as being an
   * <i>intermediate</i> operation most of which actually check for stream termination/closure.
   * TC Store is adopting the Java 9 behavior even in a Java 1.8 environment.
   */
  @Test
  public void testBaseStreamOnClose() throws Exception {
    S stream = close(getStream());
    assertThrows(() -> stream.onClose(() -> { }), IllegalStateException.class);
  }

  @Test
  public void testBaseStreamIterator() throws Exception {
    S stream = close(getStream());
    assertThrows(stream::iterator, IllegalStateException.class);
  }

  @Test
  public void testBaseStreamSpliterator() throws Exception {
    S stream = close(getStream());
    assertThrows(stream::spliterator, IllegalStateException.class);
  }

  private static final int JAVA_MAJOR;
  static {
    Matcher matcher = Pattern.compile("([1-9][0-9]*(?:(?:\\.0)*\\.[1-9][0-9]*)*).*")
        .matcher(System.getProperty("java.version"));
    if (matcher.matches()) {
      String[] versionComponents = matcher.group(1).split("\\.");
      int major = Integer.parseInt(versionComponents[0]);
      if (major > 1) {
        JAVA_MAJOR = major;
      } else {
        JAVA_MAJOR = Integer.parseInt(versionComponents[1]);
      }
    } else {
      throw new IllegalStateException("Unexpected parse failure on java.version " + matcher);
    }
  }

  /**
   * Get the "major" Java runtime version.  The version numbering scheme for Java changes
   * between Java 1.8 and Java 9 -- Java 9 drops the leading "1" (among other changes).
   * @return the major version of the current Java runtime
   */
  protected int javaMajor() {
    return JAVA_MAJOR;
  }

  protected abstract S getStream();

  protected S close(S stream) {
    stream.close();
    return stream;
  }

  @SuppressWarnings("Duplicates")
  protected static <T extends Exception> void assertThrows(Procedure proc, Class<T> expected) {
    try {
      proc.invoke();
      fail("Expecting " + expected.getSimpleName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }

  @FunctionalInterface
  protected interface Procedure {
    void invoke();
  }
}
