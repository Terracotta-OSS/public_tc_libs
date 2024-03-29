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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Alex Snaps
 */
public class VicariousThreadLocal<T> extends ThreadLocal<T> {
  /**
   * Maps a unique WeakReference onto each Thread.
   */
  private static final ThreadLocal<WeakReference<Thread>> weakThread =
      new ThreadLocal<>();

  /**
   * Returns a unique object representing the current thread.
   * Although we use a weak-reference to the thread,
   * we could use practically anything
   * that does not reference our class-loader.
   */
  static WeakReference<Thread> currentThreadRef() {
    WeakReference<Thread> ref = weakThread.get();
    if (ref == null) {
      ref = new WeakReference<>(Thread.currentThread());
      weakThread.set(ref);
    }
    return ref;
  }

  /**
   * Object representing an uninitialised value.
   */
  private static final Object UNINITIALISED = new Object();

  /**
   * Actual ThreadLocal implementation object.
   */
  private final ThreadLocal<WeakReference<Holder>> local =
      new ThreadLocal<>();

  /**
   * Maintains a strong reference to value for each thread,
   * so long as the Thread has not been collected.
   * Note, alive Threads strongly references the WeakReference&lt;Thread>
   * through weakThread.
   */
  private volatile Holder strongRefs;

  /**
   * Compare-and-set of {@link #strongRefs}.
   */
  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<VicariousThreadLocal, Holder> strongRefsUpdater =
      AtomicReferenceFieldUpdater.newUpdater(VicariousThreadLocal.class, Holder.class, "strongRefs");

  /**
   * Queue of Holders belonging to exited threads.
   */
  private final ReferenceQueue<Object> queue = new ReferenceQueue<>();

  /**
   * Creates a new {@code VicariousThreadLocal}.
   */
  public VicariousThreadLocal() {
    //
  }

  @SuppressWarnings("unchecked")
  @SuppressFBWarnings("NP")
  @Override
  public T get() {
    final Holder holder;
    WeakReference<Holder> ref = local.get();
    if (ref != null) {
      holder = ref.get();
      Object value = holder.value;
      if (value != UNINITIALISED) {
        return (T)value;
      }
    } else {
      holder = createHolder();
    }
    T value = initialValue();
    holder.value = value;
    return value;
  }

  @Override
  @SuppressFBWarnings("NP")
  public void set(T value) {
    WeakReference<Holder> ref = local.get();
    final Holder holder =
        ref != null ? ref.get() : createHolder();
    holder.value = value;
  }

  /**
   * Creates a new holder object, and registers it appropriately.
   * Also polls for thread-exits.
   */
  private Holder createHolder() {
    poll();
    Holder holder = new Holder(queue);
    WeakReference<Holder> ref = new WeakReference<>(holder);

    Holder old;
    do {
      old = strongRefs;
      holder.next = old;
    } while (!strongRefsUpdater.compareAndSet(this, old, holder));

    local.set(ref);
    return holder;
  }

  @Override
  @SuppressFBWarnings("NP")
  public void remove() {
    WeakReference<Holder> ref = local.get();
    if (ref != null) {
      ref.get().value = UNINITIALISED;
    }
  }

  /**
   * Check if any strong references need should be removed due to thread exit.
   */
  public void poll() {
    synchronized (queue) {
      // Remove queued references.
      // (Is this better inside or out?)
      if (queue.poll() == null) {
        // Nothing to do.
        return;
      }
      while (queue.poll() != null) {
        ; // Discard.
      }

      // Remove any dead holders.
      Holder first = strongRefs;
      if (first == null) {
        // Unlikely...
        return;
      }
      Holder link = first;
      Holder next = link.next;
      while (next != null) {
        if (next.get() == null) {
          next = next.next;
          link.next = next;
        } else {
          link = next;
          next = next.next;
        }
      }

      // Remove dead head, possibly.
      if (first.get() == null) {
        if (!strongRefsUpdater.weakCompareAndSet(
            this, first, first.next
        )) {
          // Something else has come along.
          // Just null out - next search will remove it.
          first.value = null;
        }
      }
    }
  }

  /**
   * Holds strong reference to a thread-local value.
   * The WeakReference is to a thread-local representing the current thread.
   */
  private static class Holder extends WeakReference<Object> {
    /**
     * Construct a new holder for the current thread.
     */
    Holder(ReferenceQueue<Object> queue) {
      super(currentThreadRef(), queue);
    }

    /**
     * Next holder in chain for this thread-local.
     */
    Holder next;
    /**
     * Current thread-local value.
     * {@link #UNINITIALISED} represents an uninitialised value.
     */
    Object value = UNINITIALISED;
  }
}
