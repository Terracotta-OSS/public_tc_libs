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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.store.common.dataset.stream.PipelineMetaData;
import com.terracottatech.store.common.dataset.stream.WrappedStream;
import com.terracottatech.sovereign.impl.indexing.SimpleIndex;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexing;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.ShardedBtreeIndexMap;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Record;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Provides methods used in testing {@code Spliterator} and index usage in Sovereign.
 *
 * @author Clifford W. Johnson
 */
public final class SpliteratorUtility {
  private SpliteratorUtility() {
  }

  /**
   * Counts the splits that may be performed over a {@code Spliterator}.  An unsplitable
   * spliterator returns zero (0).  A spliterator that can be split twice (original spliterator
   * splits into itself and a second spliterator; each of the original and the second spliterators
   * split again) returns three (3).
   *
   * @param spliterator the {@code Spliterator} to try splitting
   * @param <T> the element type of the {@code Spliterator}
   * @return the number of times {@code spliterator}, and its splits, can be split
   */
  public static <T> int countSplits(final Spliterator<T> spliterator) {
    final Deque<Spliterator<T>> stack = new ArrayDeque<>();

    int splitCount = 0;
    Spliterator<T> root;
    stack.push(spliterator);
    do {
      root = stack.pop();
      Spliterator<T> fragment;
      while ((fragment = root.trySplit()) != null) {
        splitCount++;
        stack.push(fragment);
      }
    } while (!stack.isEmpty());

    return splitCount;
  }

  /**
   * Adds a terminal action to the {@code WrappedStream} supplied that gets the indexed {@code CellDefinition}
   * and {@code ManagedAction} from the {@code Spliterator} assigned to the stream.  To be effective, this
   * method must be called on the {@code WrappedStream} <i>before</i> the terminal action is invoked.
   * <p/>
   * The following code segment represents a typical usage pattern:
   * <pre>{@code
   *  try (final Stream<Record<String>> stream = dataset.records()) {
   *    final AtomicReference<CellDefinition<? extends Comparable<?>>> assignedIndexCell = new AtomicReference<>();
   *    final AtomicReference<ManagedAction<?>> assignedManagedAction = new AtomicReference<>();
   *    SpliteratorUtility.setStreamInfoAction(dataset, assignedIndexCell, assignedManagedAction);
   *
   *    final Consumer<Record<String>> mutation =
   *      dataset.applyMutation(Durability.IMMEDIATE, RecordFunctions.write(OBSERVATIONS,
   *        (Function<Record<String>, Long>)cells -> 1 + cells.get(OBSERVATIONS).orElse(0L)));
   *    stream.filter(compare(TAXONOMIC_CLASS).is("reptile")).forEach(mutation);
   *
   *    assertThat(assignedIndexCell.get(), is(equalTo(TAXONOMIC_CLASS)));
   *    assertThat(assignedManagedAction.get(), is(sameInstance(mutation)));
   *  }
   * }</pre>
   *
   * @param dataset the {@code SovereignDataset} from which {@code stream} is created
   * @param stream the {@code WrappedStream} instance to observe
   * @param indexedCellRef the {@code AtomicReference} into which the {@code CellDefinition} for the
   * assigned index is set; the result will be {@code null} if an index is not
   * selected for the stream
   * @param managedActionRef the {@code AtomicReference} into which the {@code ManagedAction} for
   * the stream is set; the result will be {@code null} if no {@code ManagedAction}
   */
  public static <K extends Comparable<K>> void setStreamInfoAction(
    final SovereignDataset<K> dataset, final Stream<Record<K>> stream,
    final AtomicReference<CellDefinition<? extends Comparable<?>>> indexedCellRef,
    final AtomicReference<ManagedAction<? extends Comparable<?>>> managedActionRef) {

    @SuppressWarnings("unchecked") final WrappedStream<Record<K>, Stream<Record<K>>> wrappedStream = (WrappedStream<Record<K>, Stream<Record<K>>>) stream;
    final Consumer<PipelineMetaData> initialTerminalAction = wrappedStream.getMetaData().getPipelineConsumer();
    assertThat(initialTerminalAction, is(instanceOf(Supplier.class)));

    wrappedStream.appendTerminalAction(m -> {
      @SuppressWarnings("unchecked") final Spliterator<Record<K>> spliterator = ((Supplier<Spliterator<Record<K>>>) initialTerminalAction).get();
      if (indexedCellRef != null) {
        indexedCellRef.set(SpliteratorUtility.getIndexedOn(dataset, spliterator));
      }
      if (managedActionRef != null) {
        managedActionRef.set(SpliteratorUtility.getManagedAction(dataset, spliterator));
      }
    });
  }

  /**
   * Determines the {@code CellDefinition}, if any, of the index used by the {@code Spliterator}
   * provided.
   * <p>This method relies on reflection and implementation details to mapMethods the result.  The
   * method used to determine the index used fails to determine the index if the filter expression
   * used to select the index has no hits in the index.
   *
   * @param dataset the {@code SovereignDataset}
   * @param spliterator the {@code Spliterator} for which the indexed {@code CellDefinition} is to be
   * determined
   * @param <K> the type of the {@code Record} key values from {@code dataset}
   * @return {@code null} if {@code spliterator} is using the primary index; otherwise, a reference to
   * the {@code CellDefinition} of the index used by {@code spliterator}
   */
  public static <K extends Comparable<K>> CellDefinition<? extends Comparable<?>> getIndexedOn(
    final SovereignDataset<K> dataset, final Spliterator<Record<K>> spliterator) {
    assert dataset != null;
    assert spliterator != null;

    if (!(spliterator instanceof ManagedSpliterator)) {
      return null;
    }

    PersistentMemoryLocator loc = ((ManagedSpliterator) spliterator).getCurrent();
    SovereignSortedIndexMap<?, ?> map = ShardedBtreeIndexMap.internalBtreeFor(loc);
    if (map != null) {
      if (dataset.getIndexing() instanceof SimpleIndexing) {
        SimpleIndex<?, ?> ii = ((SimpleIndexing<?>) dataset.getIndexing()).indexFor(map);
        if (ii != null) {
          return ii.on();
        }
      }
    }
    return null;
    // CRSS Holy hell, Batman!
  }

  /**
   * Gets the {@code ManagedAction}, if any, associated with the {@code Spliterator} provided.
   * <p>This method relies on reflection and implementation details to mapMethods the result.
   *
   * @param dataset the {@code SovereignDataset} from which the {@code Spliterator} was obtained
   * @param spliterator the {@code Spliterator} for which the {@code ManagedAction} is sought
   * @param <K> the type of the {@code Record} key values from {@code dataset}
   * @return {@code null} if {@code spliterator} is has no associated {@code ManagedAction}; otherwise
   * a reference to the {@code ManagedAction}
   */
  @SuppressWarnings("unchecked")
  public static <K extends Comparable<K>> ManagedAction<K> getManagedAction(
    final SovereignDataset<K> dataset, final Spliterator<Record<K>> spliterator) {
    assert dataset != null;
    assert spliterator != null;

    if (!(spliterator instanceof ManagedSpliterator)) {
      return null;
    }

    try {
      final Field managedActionField = ManagedSpliterator.class.getDeclaredField("manager");
      managedActionField.setAccessible(true);
      return (ManagedAction<K>) managedActionField.get(spliterator);   // unchecked

    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  public static final class IndexAssociationError extends AssertionError {
    private static final long serialVersionUID = -5017214056932199183L;

    public IndexAssociationError(final Object detailMessage) {
      super(detailMessage);
    }

    public IndexAssociationError(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
