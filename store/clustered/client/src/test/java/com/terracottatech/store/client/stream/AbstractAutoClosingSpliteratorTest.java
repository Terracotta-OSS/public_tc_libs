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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.terracottatech.test.data.Animals;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.BaseStream;

import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Exercises methods in {@link AbstractAutoClosingSpliterator}.
 */
public class AbstractAutoClosingSpliteratorTest {

  @Mock
  private BaseStream<String, ?> headStream;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testClose() {
    TestSpliterator spliterator = new TestSpliterator(getSpliterator(), headStream);
    spliterator.close();
    verify(headStream).close();
  }

  @Test
  public void testTryAdvance() {
    TestSpliterator spliterator = new TestSpliterator(new TestRemoteSpliterator<>(Spliterators.emptySpliterator()), headStream);
    assertThat(spliterator.tryAdvance(s -> { }), is(false));
    verify(headStream).close();
  }

  @SuppressWarnings("StatementWithEmptyBody")
  @Test
  public void testTryAdvanceExhaustion() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    AtomicInteger counter = new AtomicInteger();
    do { } while (spliterator.tryAdvance(s -> counter.incrementAndGet()));
    assertThat(counter.get(), is((int)stream(getSpliterator(), false).count()));
    verify(headStream).close();
  }

  @Test
  public void testForEachRemaining() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    AtomicInteger counter = new AtomicInteger();
    spliterator.forEachRemaining(s -> counter.getAndIncrement());
    assertThat(counter.get(), is((int)stream(getSpliterator(), false).count()));
    verify(headStream).close();
  }

  @Test
  public void testTrySplitFail() {
    RemoteSpliterator<String> delegate = getUnsplittableSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    assertThat(spliterator.trySplit(), is(nullValue()));
    verify(headStream, never()).close();
  }

  @Test
  public void testTrySplitSuccess() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);

    Spliterator<String> split = spliterator.trySplit();

    assertThat(split, is(notNullValue()));

    split.tryAdvance(e -> {});
    spliterator.tryAdvance(e -> {});
    verify(headStream, never()).close();

    split.forEachRemaining(e -> {});
    verify(headStream, never()).close();
    spliterator.forEachRemaining(e -> {});
    verify(headStream).close();
  }

  @Test
  public void testEstimateSize() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    assertThat(spliterator.estimateSize(), is(delegate.estimateSize()));
    verify(headStream, never()).close();
  }

  @Test
  public void testGetExactSizeIfKnown() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    assertThat(spliterator.getExactSizeIfKnown(), is(delegate.getExactSizeIfKnown()));
    verify(headStream, never()).close();
  }

  @Test
  public void testCharacteristics() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    assertThat(spliterator.characteristics(), is(delegate.characteristics()));
    verify(headStream, never()).close();
  }

  @Test
  public void testHasCharacteristics() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    assertThat(delegate.hasCharacteristics(Spliterator.SIZED), is(true));
    assertThat(spliterator.hasCharacteristics(Spliterator.SIZED), is(delegate.hasCharacteristics(Spliterator.SIZED)));
    assertThat(delegate.hasCharacteristics(Spliterator.DISTINCT), is(false));
    assertThat(spliterator.hasCharacteristics(Spliterator.DISTINCT), is(delegate.hasCharacteristics(Spliterator.DISTINCT)));
    verify(headStream, never()).close();
  }

  @Test
  public void testGetComparator() {
    RemoteSpliterator<String> delegate = getSpliterator();
    TestSpliterator spliterator = new TestSpliterator(delegate, headStream);
    try {
      Comparator<? super String> delegateComparator = delegate.getComparator();
      assertThat(spliterator.getComparator(), is(sameInstance(delegateComparator)));
    } catch (IllegalStateException e) {
      assertThrows(spliterator::getComparator, IllegalStateException.class);
    } finally {
      verify(headStream, never()).close();
    }
  }

  private RemoteSpliterator<String> getSpliterator() {
    // parallel() makes it splittable ...
    return new TestRemoteSpliterator<>(Animals.ANIMALS.stream().map(Animals.Animal::getName).parallel().spliterator());
  }

  @Test
  public void checkSplittableStream() {
    assertThat(getSpliterator().trySplit(), notNullValue());
  }

  private RemoteSpliterator<String> getUnsplittableSpliterator() {
    // If the stream is sequential() and the spliterator is *not* taken from the head stream then the stream
    // implementation makes the spliterator unsplittable.
    return new TestRemoteSpliterator<>(Animals.ANIMALS.stream().map(Animals.Animal::getName).sequential().spliterator());
  }

  @Test
  public void checkUnsplittableStream() {
    assertThat(getUnsplittableSpliterator().trySplit(), nullValue());
  }

  private static final class TestSpliterator
      extends AbstractAutoClosingSpliterator<String, RemoteSpliterator<String>, Spliterator<String>> {

    TestSpliterator(RemoteSpliterator<String> delegate, BaseStream<?, ?> headStream) {
      super(delegate, headStream);
    }

    private TestSpliterator(TestSpliterator owner, Spliterator<String> split) {
      super(owner, split);
    }

    @Override
    protected Spliterator<String> chainedSpliterator(Spliterator<String> split) {
      return new TestSpliterator(this, split);
    }
  }

  @SuppressWarnings("Duplicates")
  private static <T extends Exception> void assertThrows(Runnable proc, Class<T> expected) {
    try {
      proc.run();
      fail("Expecting " + expected.getSimpleName());
    } catch (Exception t) {
      if (!expected.isInstance(t)) {
        throw t;
      }
    }
  }
}