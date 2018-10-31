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

package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.Serializable;
import java.util.Comparator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Provides a base for the construction of JUnit tests for
 * {@link com.terracottatech.sovereign.spi.store.DataContainer DataContainer}
 * implementations.
 *
 * @author Clifford W. Johnson
 */
public abstract class AbstractDataContainerTest<W extends Comparable<W>> {

  @Mock
  private PersistentMemoryLocator locator;

  /**
   * Initializes fields annotated with {@link org.mockito.Mock @Mock}.
   */
  @Before
  public final void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Gets the {@link com.terracottatech.sovereign.spi.store.DataContainer} implementation to test.
   *
   * @return the {@code DataContainer} implementation to test
   */
  protected abstract SovereignContainer<W> getContainer();

  /**
   * Gets a test value.
   *
   * @return a value of the type {@code <W>}
   */
  protected abstract VersionedRecord<W> getValue();

  @Test
  public final void testDispose() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    assertFalse(container.isDisposed());
    container.dispose();
    assertTrue(container.isDisposed());
  }

  @Test
  public final void testDisposedAdd() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    VersionedRecord<W> value = this.getValue();
    if (!Mockito.mockingDetails(value).isMock()) {
      value = spy(this.getValue());
    }
    try {
      container.add(value);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(value);
  }

  @Test
  public void testDisposedDelete() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    try {
      container.delete(this.locator);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.locator);
  }

  @Test
  public void testDisposedDispose() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    container.dispose();    // Second dispose should not fail
  }

  @Test
  public void testDisposedDrop() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    try {
      container.drop();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedEnd() throws Exception {
    try(ContextImpl context = spy(new ContextImpl(null, false))) {

      SovereignContainer<W> container = this.getContainer();
      container.dispose();

      container.end(context);
      verify(context).close();
    }
  }

  @Test
  public void testDisposedFirst() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    try (ContextImpl c = container.start(true)) {
      container.first(c);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedGet() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    try {
      container.get(this.locator);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(this.locator);
  }

  @Test
  public void testDisposedLast() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    try {
      container.last();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDisposedReplace() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    VersionedRecord<W> value = this.getValue();
    if (!Mockito.mockingDetails(value).isMock()) {
      value = spy(this.getValue());
    }
    try {
      container.replace(this.locator, value);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    verifyZeroInteractions(value);
    verifyZeroInteractions(this.locator);
  }

  @Test
  public void testDisposedStart() throws Exception {
    SovereignContainer<W> container = this.getContainer();
    container.dispose();

    try {
      container.start(true);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  /**
   * A {@link TimeReference} implementation where the time reference is directly set
   * by the consumer.
   */
  protected static class DirectTimeReference implements TimeReference<DirectTimeReference>, Serializable {
    private static final long serialVersionUID = 2289435479223761990L;

    private static final Comparator<DirectTimeReference> COMPARATOR = Comparator.comparingLong(t -> t.timeReference);

    private final long timeReference;

    private DirectTimeReference() {
      this.timeReference = 0;
    }

    private DirectTimeReference(final long timeReference) {
      this.timeReference = timeReference;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compareTo(final TimeReference t) {
      return COMPARATOR.compare(this, (DirectTimeReference) t);
    }

    public static DirectTimeReference instance(final long timeReference) {
      return new DirectTimeReference(timeReference);
    }

    /**
     * {@code TimeReferenceGenerator} producing a <i>bad</i> {@code DirectTimeReference} instance.
     * All functional instances of {@code DirectTimeReference} must be allocated using
     * {@link DirectTimeReference#instance(long)}.
     */
    public static final class Generator implements TimeReferenceGenerator<DirectTimeReference> {
      private static final long serialVersionUID = 3035534911031001718L;

      @Override
      public Class<DirectTimeReference> type() {
        return DirectTimeReference.class;
      }

      @Override
      public DirectTimeReference get() {
        return new DirectTimeReference() {
          private static final long serialVersionUID = -4168729372807426010L;
          @SuppressWarnings("rawtypes")
          @Override
          public int compareTo(final TimeReference t) {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public int maxSerializedLength() {
        return 0;
      }
    }
  }
}
