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
package com.terracottatech.sovereign.common.utils;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

public class SimpleFinalizerTest {

  @Test(expected = AssertionError.class)
  public void testFinalizerOfCustomEqualType() {
    new SimpleFinalizer<>(String.class, s -> {});
  }

  @Test
  public void testFinalizerOfCleanType() {
    new SimpleFinalizer<>(Thread.class, s -> {});
  }

  @SuppressWarnings("try")
  @Test
  public void testTaskIsRunForReleasedObject() throws InterruptedException {
    Semaphore semaphore = new Semaphore(0);

    SimpleFinalizer<Object, Object> finalizer = new SimpleFinalizer<>(Object.class, 100, TimeUnit.MILLISECONDS, s -> semaphore.release());

    finalizer.record(new Object(), new Object());

    try (GcHelper ignored = new GcHelper()) {
      assertTrue(semaphore.tryAcquire(30, SECONDS));
    }
  }

  @SuppressWarnings("try")
  @Test
  public void testTaskIsNotRunForRemovedObject() throws InterruptedException {
    Semaphore semaphore = new Semaphore(0);

    SimpleFinalizer<Object, Object> finalizer = new SimpleFinalizer<>(Object.class, 100, TimeUnit.MILLISECONDS, s -> semaphore.release());

    {
      Object marker = new Object();
      Object object = new Object();
      finalizer.record(marker, object);
      finalizer.remove(object);
    }

    try (GcHelper ignored = new GcHelper()) {
      assertFalse(semaphore.tryAcquire(2, SECONDS));
    }
  }

  static class GcHelper implements AutoCloseable {

    private final Thread runner = new Thread(() -> {
      while (!Thread.interrupted()) {
        System.gc();
        Thread.yield();
      }
    });

    public GcHelper() {
      runner.start();
    }

    @Override
    public void close() {
      runner.interrupt();
    }
  }
}