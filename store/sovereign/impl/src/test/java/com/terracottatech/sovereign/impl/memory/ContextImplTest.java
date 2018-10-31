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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.common.utils.SimpleFinalizer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 * @author Clifford W. Johnson
 */
public class ContextImplTest {

  @Mock
  private AbstractRecordContainer<String> mockContainer;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Ensures that the finalization pattern used to release resources of an unclosed-but-reclaimed
   * {@link ContextImpl} is effective.
   */
  @SuppressWarnings("UnusedAssignment")
  @Test(timeout = 60000L)
  public void testContextImplFinalize() throws Exception {
    final ReferenceQueue<ContextImpl> queue = new ReferenceQueue<>();

    final long finalizerRunMillis = TimeUnit.SECONDS.toMillis(2L);

    final Thread mainThread = Thread.currentThread();
    final SimpleFinalizer<ContextImpl, ContextImpl.State> finalizer = new SimpleFinalizer<>(ContextImpl.State.class, finalizerRunMillis,
      TimeUnit.MILLISECONDS, s -> {
      if (s.isOpen()) {
        s.close();
        mainThread.interrupt();
      }
    });

    try {
      when(this.mockContainer.getContextFinalizer()).thenReturn(finalizer);
      ContextImpl context = new ContextImpl(this.mockContainer, true);

      final AtomicBoolean finalized = new AtomicBoolean();
      context.addCloseable(() -> finalized.set(true));

      final PhantomReference<ContextImpl> ref = new PhantomReference<>(context, queue);

      assertThat(finalized.get(), is(false));

      context = null;   // unusedAssignment - explicit null of reference for garbage collection

      /*
       * Await garbage collection of the context.
       */
      Reference<? extends ContextImpl> queuedRef;
      while ((queuedRef = queue.poll()) == null) {
        System.gc();
        Thread.sleep(100L);
      }
      assertThat(queuedRef, is(sameInstance(ref)));
      queuedRef.clear();

      /*
       * Now wait for the SimpleFinalizer thread to make the call.
       */
      while (true) {
        try {
          Thread.sleep(100L);
        } catch (InterruptedException e) {
          // ignore
        }
        if (finalized.get()) {
          break;
        }
      }

    } finally {
      finalizer.close();
    }
  }

  /**
   * Ensures that the finalization pattern used to release resources of an unclosed-but-reclaimed
   * {@link ContextImpl} is cleaned up when closing a {@code ContextImpl}.
   */
  @SuppressWarnings("UnusedAssignment")
  @Test(timeout = 30000L)
  public void testContextImplClosed() throws Exception {
    final ReferenceQueue<ContextImpl> queue = new ReferenceQueue<>();

    final long finalizerRunMillis = TimeUnit.SECONDS.toMillis(10L);

    final Thread mainThread = Thread.currentThread();
    final AtomicBoolean firedForState = new AtomicBoolean();
    final AtomicReference<ContextImpl.State> stateRef = new AtomicReference<>(null);
    final SimpleFinalizer<ContextImpl, ContextImpl.State> finalizer = new SimpleFinalizer<>(ContextImpl.State.class, finalizerRunMillis,
      TimeUnit.MILLISECONDS, s -> {
      if (stateRef.get() == s) {
        firedForState.set(true);
      }
      mainThread.interrupt();
    });

    try {
      when(this.mockContainer.getContextFinalizer()).thenReturn(finalizer);
      ContextImpl context = new ContextImpl(this.mockContainer, true);
      stateRef.set(context.getState());

      final AtomicBoolean closed = new AtomicBoolean();
      context.addCloseable(() -> closed.set(true));

      final PhantomReference<ContextImpl> ref = new PhantomReference<>(context, queue);

      assertThat(closed.get(), is(false));
      context.close();
      assertThat(closed.get(), is(true));

      context = null;   // unusedAssignment - explicit null of reference for garbage collection

      /*
       * Await garbage collection of the context.
       */
      Reference<? extends ContextImpl> queuedRef;
      while ((queuedRef = queue.poll()) == null) {
        System.gc();
        Thread.sleep(100L);
      }
      assertThat(queuedRef, is(sameInstance(ref)));
      queuedRef.clear();

      /*
       * Now wait for the SimpleFinalizer thread to make the call.
       */
      for (int i = 0; i < 10; i++) {
        try {
          Thread.sleep(finalizerRunMillis / 10);
        } catch (InterruptedException e) {
          // ignore
        }
        if (firedForState.get()) {
          break;
        }
      }

      assertThat(firedForState.get(), is(false));

    } finally {
      finalizer.close();
    }
  }

}
