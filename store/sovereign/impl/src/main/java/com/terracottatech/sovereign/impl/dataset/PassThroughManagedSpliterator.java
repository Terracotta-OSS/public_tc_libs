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
package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.store.Record;
import com.terracottatech.store.internal.InternalRecord;

import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A {@link Spliterator} implementation providing a <i>managed</i> wrapper over another
 * {@code Spliterator}.
 *
 * @implNote Even when a {@code Spliterator} is supplied, when obtained from
 * {@link Stream#spliterator()}, the instance may be obtained using a {@link Supplier}; the
 * reference should not be accessed until actually needed to prevent premature allocation of
 * the {@code Spliterator}.
 *
 * @author Clifford W. Johnson
 */
final class PassThroughManagedSpliterator<K extends Comparable<K>>
    implements Spliterator<Record<K>>, AutoCloseable {

  private final Supplier<ManagedAction<K>> managedActionSupplier;
  private volatile ManagedAction<K> managedAction;
  private final Spliterator<Record<K>> spliterator;

  /**
   * Create a {@code PassThroughManagedSpliterator} using the {@code Spliterator} provided and the
   * {@code ManagedAction} provided through the {@code Supplier} provided.  The {@link Supplier#get()}
   * method will be called no more than once to obtain the {@code ManagedAction}.
   *
   * @param spliterator the {@code Spliterator} instance used to provide elements for this {@code spliterator};
   *                    may not be {@code null}
   * @param managedActionSupplier the {@code Supplier} from which the {@code ManagedAction} instance used to
   *                              open and close stream elements is obtained; may not be {@code null}
   *
   * @throws NullPointerException if either {@code spliterator} or {@code manager} is {@code null}
   */
  public PassThroughManagedSpliterator(
      final Spliterator<Record<K>> spliterator,
      final Supplier<ManagedAction<K>> managedActionSupplier) {
    Objects.requireNonNull(spliterator, "spliterator");
    Objects.requireNonNull(managedActionSupplier, "managedActionSupplier");
    this.spliterator = spliterator;
    this.managedActionSupplier = managedActionSupplier;
  }

  /**
   * {@inheritDoc}
   * <p>This implementation invokes the {@code tryAdvance} method of the {@code Spliterator}
   * supplied at construction.  The {@link Consumer} passed to that method wraps a call to
   * {@code action.accept} with calls to {@link #managedAction} {@code begin} and {@code end}.
   * The invocation of {@code tryAdvance} of the wrapped spliterator is repeated, possibly
   * to exhaustion, if {@link ManagedAction#begin(InternalRecord) managedAction.begin} returns {@code null}.
   *
   * @apiNote When used in construction of a {@link java.util.stream.Stream Stream},
   *    {@code action} is a stream operation.
   *
   * @param action {@inheritDoc}
   * @return {@inheritDoc}
   * @throws NullPointerException {@inheritDoc}
   */
  @Override
  public boolean tryAdvance(Consumer<? super Record<K>> action) {
    Objects.requireNonNull(action, "action");
    final ManagedAction<K> managedAction = this.getManagedAction();

    /*
     * Loop getting a Record from the source Spliterator and opening
     * that record until we actually process one or run out.  The
     * begin process re-fetches the Record by key and might come up
     * empty.
     */
    final AtomicBoolean tryNext = new AtomicBoolean(true);
    boolean processedRecord;
    do {
      processedRecord = this.spliterator.tryAdvance(r -> {
          final InternalRecord<K> current = managedAction.begin((InternalRecord<K>)r);
          try {
            if (current != null) {
              action.accept(current);
              tryNext.set(false);
            }
          } finally {
            // Unlock even if null -- lock is taken on original Record 'r'
            managedAction.end(current);
          }
        });
    } while (processedRecord && tryNext.get());

    return processedRecord;
  }

  @Override
  public Spliterator<Record<K>> trySplit() {
    // TODO: Implement something reasonable for trySplit
    return null;
  }

  @Override
  public long estimateSize() {
    return this.spliterator.estimateSize();
  }

  @Override
  public int characteristics() {
    return this.spliterator.characteristics();
  }

  @Override
  public void close() {
    final ManagedAction<K> managedAction = this.getManagedAction();
    if (managedAction != null && (managedAction instanceof AutoCloseable)) {
      try {
        ((AutoCloseable)managedAction).close();
      } catch (Exception e) {
        // Ignored
      }
    }
  }

  /**
   * Gets the {@code ManagedAction} to use while processing stream elements.  If not provided during
   * construction, the {@code ManagedAction} is obtained from {@link #managedActionSupplier} the first
   * time this method is called.
   *
   * @return the {@code ManagedAction} to use for stream element processing
   */
  private ManagedAction<K> getManagedAction() {
    ManagedAction<K> managedAction = this.managedAction;
    if (managedAction == null) {
      synchronized (this) {
        managedAction = this.managedAction;
        if (managedAction == null) {
          this.managedAction = this.managedActionSupplier.get();
          managedAction = this.managedAction;
          if (managedAction == null) {
            throw new IllegalStateException("No ManagedAction returned from managedActionSupplier");
          }
        }
      }
    }
    return managedAction;
  }
}
