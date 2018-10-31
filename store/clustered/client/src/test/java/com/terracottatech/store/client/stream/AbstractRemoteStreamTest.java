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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.WrappedStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.BaseStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Provides test platform for tests on subclasses of {@link AbstractRemoteBaseStream}.
 */
public abstract class AbstractRemoteStreamTest<K extends Comparable<K>, T, S extends BaseStream<T, S>, R extends AbstractRemoteBaseStream<K, T, S>> {

  @Rule
  public EmbeddedAnimalDataset animalDataset = new EmbeddedAnimalDataset();

  private final IntermediateOperation typeOperation;
  private final Class<R> remoteStreamClass;
  private final Class<S> nativeStreamClass;

  protected AbstractRemoteStreamTest(IntermediateOperation typeOperation, Class<R> remoteStreamClass, Class<S> nativeStreamClass) {
    this.typeOperation = typeOperation;
    this.remoteStreamClass = remoteStreamClass;
    this.nativeStreamClass = nativeStreamClass;
  }


  @Test
  public void testInstantiation() throws Exception {
    R stream = getTestStream();
    assertThat(stream, is(instanceOf(remoteStreamClass)));
    assertThat(stream, is(instanceOf(nativeStreamClass)));
  }

  @Test
  public void testOnClose() throws Exception {
    AtomicBoolean closed = new AtomicBoolean(false);
    tryStream(stream -> {
      assertThat(stream.onClose(() -> closed.set(true)), is(instanceOf(remoteStreamClass)));
      if (typeOperation == null) {
        assertPortableOps(stream);
      } else {
        assertPortableOps(stream, typeOperation);
      }
      assertNonPortableOps(stream);
    });
    assertTrue(closed.get());
  }

  @Test
  public void testClose() throws Exception {
    AtomicBoolean closed = new AtomicBoolean(false);
    tryStream(stream -> {
      stream.onClose(() -> closed.set(true));
      stream.close();
      if (typeOperation == null) {
        assertPortableOps(stream);
      } else {
        assertPortableOps(stream, typeOperation);
      }
      assertNonPortableOps(stream);
    });
    assertTrue(closed.get());
  }


  /**
   * Asserts that the portable operations calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link AbstractRemoteBaseStream} from which the portable operations are observed
   * @param operations the expected sequence of portable operations
   */
  protected void assertPortableOps(R stream, PipelineOperation.Operation... operations) {
    RootRemoteRecordStream<K> rootStream = stream.getRootStream();
    assertPortableOpsInternal(rootStream, operations);
  }

  /**
   * Asserts that the portable operations calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link RootRemoteRecordStream} from which the portable operations are observed
   * @param operations the expected sequence of portable operations
   */
  private void assertPortableOpsInternal(RootRemoteRecordStream<K> stream, PipelineOperation.Operation... operations) {
    List<PipelineOperation> portablePipeline = stream.getPortableIntermediatePipelineSequence();
    if (operations.length == 0) {
      assertThat(portablePipeline, is(empty()));
    } else {
      List<PipelineOperation.Operation> actualOperations =
          portablePipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(actualOperations, contains(operations));
    }
  }

  /**
   * Asserts that the non-portable operations calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link AbstractRemoteBaseStream} from which the non-portable operations are observed
   * @param operations the expected sequence of non-portable operations
   */
  protected void assertNonPortableOps(R stream, PipelineOperation.Operation... operations) {
    RootRemoteRecordStream<K> rootStream = stream.getRootStream();
    assertNonPortableOpsInternal(rootStream, operations);
  }

  /**
   * Asserts that the portable terminal operation calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link RootRemoteRecordStream} from which the portable operations are observed
   * @param terminalOperation the expected portable terminal operation
   */
  protected void assertTerminalPortableOp(R stream, PipelineOperation.TerminalOperation terminalOperation) {
    RootRemoteRecordStream<K> rootStream = stream.getRootStream();
    PipelineOperation portableTerminalOperation = rootStream.getPortableTerminalOperation();
    if (terminalOperation == null) {
      assertThat(portableTerminalOperation, is(nullValue()));
    } else {
      assertThat(portableTerminalOperation.getOperation(), is(terminalOperation));
    }
  }

  /**
   * Asserts that the non-portable operations calculated by pipeline analysis matches the expected sequence.
   * @param stream the {@link RootRemoteRecordStream} from which the non-portable operations are observed
   * @param operations the expected sequence of non-portable operations
   */
  private void assertNonPortableOpsInternal(RootRemoteRecordStream<K> stream, PipelineOperation.Operation... operations) {
    WrappedStream<?, ?> rootWrappedStream = stream.getRootWrappedStream();
    List<PipelineOperation> nonPortablePipeline =
        (rootWrappedStream == null ? Collections.emptyList() : rootWrappedStream.getMetaData().getPipeline());
    if (operations.length == 0) {
      assertThat(nonPortablePipeline, is(empty()));
    } else {
      List<PipelineOperation.Operation> actualOperations =
          nonPortablePipeline.stream().map(PipelineOperation::getOperation).collect(toList());
      assertThat(actualOperations, contains(operations));
    }
  }

  protected Matcher<double[]> doubleArrayContainsInAnyOrder(double[] matching) {
    final ArrayList<Matcher<Double>> matchers = new ArrayList<>();
    for (Double val : matching) {
      matchers.add(is(val));
    }
    return new TypeSafeMatcher<double[]>() {
      private final ArrayList<Matcher<Double>> internalMatchers = new ArrayList<>(matchers);
      private double nonMatch;

      @Override
      public void describeTo(Description description) {
        description.appendList("[", ", ", "]", internalMatchers)
            .appendText(" in any order ");
      }

      @Override
      protected boolean matchesSafely(double[] doubles) {
        if (doubles.length != internalMatchers.size()) {
          return false;
        }
        for (double aDouble : doubles) {
          Matcher<Double> matchingMatcher = null;
          for (Matcher<Double> matcher : internalMatchers) {
            if (matcher.matches(aDouble)) {
              matchingMatcher = matcher;
              break;
            }
          }
          if (matchingMatcher != null) {
            internalMatchers.remove(matchingMatcher);
          } else {
            nonMatch = aDouble;
            return false;
          }
        }
        return internalMatchers.isEmpty();
      }
    };
  }

  protected Matcher<int[]> intArrayContainsInAnyOrder(int[] matching) {
    final ArrayList<Matcher<Integer>> matchers = new ArrayList<>();
    for (Integer val : matching) {
      matchers.add(is(val));
    }
    return new TypeSafeMatcher<int[]>() {
      private final ArrayList<Matcher<Integer>> internalMatchers = new ArrayList<>(matchers);
      private double nonMatch;

      @Override
      public void describeTo(Description description) {
        description.appendList("[", ", ", "]", internalMatchers)
            .appendText(" in any order ");
      }

      @Override
      protected boolean matchesSafely(int[] ints) {
        if (ints.length != internalMatchers.size()) {
          return false;
        }
        for (int anInt : ints) {
          Matcher<Integer> matchingMatcher = null;
          for (Matcher<Integer> matcher : internalMatchers) {
            if (matcher.matches(anInt)) {
              matchingMatcher = matcher;
              break;
            }
          }
          if (matchingMatcher != null) {
            internalMatchers.remove(matchingMatcher);
          } else {
            nonMatch = anInt;
            return false;
          }
        }
        return internalMatchers.isEmpty();
      }
    };
  }

  protected Matcher<long[]> longArrayContainsInAnyOrder(long[] matching) {
    final ArrayList<Matcher<Long>> matchers = new ArrayList<>();
    for (Long val : matching) {
      matchers.add(is(val));
    }
    return new TypeSafeMatcher<long[]>() {
      private final ArrayList<Matcher<Long>> internalMatchers = new ArrayList<>(matchers);
      private double nonMatch;

      @Override
      public void describeTo(Description description) {
        description.appendList("[", ", ", "]", internalMatchers)
            .appendText(" in any order ");
      }

      @Override
      protected boolean matchesSafely(long[] longs) {
        if (longs.length != internalMatchers.size()) {
          return false;
        }
        for (long aLong : longs) {
          Matcher<Long> matchingMatcher = null;
          for (Matcher<Long> matcher : internalMatchers) {
            if (matcher.matches(aLong)) {
              matchingMatcher = matcher;
              break;
            }
          }
          if (matchingMatcher != null) {
            internalMatchers.remove(matchingMatcher);
          } else {
            nonMatch = aLong;
            return false;
          }
        }
        return internalMatchers.isEmpty();
      }
    };
  }

  protected static <T> Matcher<Iterable<? extends T>> isOrderedAccordingTo(final Comparator<? super T> comparator) {
    return new TypeSafeMatcher<Iterable<? extends T>>() {
      @Override
      protected boolean matchesSafely(Iterable<? extends T> iterable) {
        Iterator<? extends T> iterator = iterable.iterator();
        if (!iterator.hasNext()) {
          return false;
        }
        T previous = iterator.next();
        while (iterator.hasNext()) {
          if (comparator.compare(previous, previous = iterator.next()) > 0) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" matching comparator ordering");
      }
    };
  }

  /**
   * Runs a test, packaged as a {@link Consumer}, against the {@link RootRemoteRecordStream} obtained from
   * {@link #getTestStream()}.
   * @param testProc the test procedure
   */
  protected void tryStream(Consumer<R> testProc) throws Exception {
    try (final R stream = getTestStream()) {
      testProc.accept(stream);
    }
  }

  protected abstract R getTestStream();

  protected abstract S getExpectedStream();

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
