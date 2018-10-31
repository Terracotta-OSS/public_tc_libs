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
package com.terracottatech.sovereign;

import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.sovereign.exceptions.RecordLockedException;
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.indexing.SovereignIndexStatistics;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.time.TimeReference;
import com.terracottatech.sovereign.time.TimeReferenceGenerator;
import com.terracottatech.sovereign.utils.MiscUtils;
import com.terracottatech.store.Cell;
import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 * A collection of uniquely keyed {@link Record} instances.
 * <p/>
 * A {@code SovereignDataset} instance is constructed using a
 * {@link com.terracottatech.sovereign.impl.SovereignBuilder SovereignBuilder} instance.
 * <p/>
 * Methods in this class may throw an {@link IllegalStateException} if used after the
 * {@link #dispose() dispose} method is called.
 * <p/>
 * Methods in this class may throw a
 * {@link com.terracottatech.sovereign.exceptions.PersistenceFailureException PersistenceFailureException}
 * (an unchecked {@code Exception}) to indicate the failure in persistence of a mutation operation.  For
 * operations specifying
 * {@link com.terracottatech.sovereign.SovereignDataset.Durability#IMMEDIATE Durability.IMMEDIATE}
 * ({@code add}, {@code applyMutation}, or {@code delete}),
 * the {@code PersistenceFailureException} will be thrown coincident to invocation of the method.
 * For operations specifying {@link com.terracottatech.sovereign.SovereignDataset.Durability#LAZY Durability.LAZY},
 * the exception will be thrown on invocation of the first data access method
 * ({@code add}, {@code applyMutation}, {@code delete}, {@code records}, {@code get}, or {@code dispose})
 * following recognition of the failure.
 *
 * <h3>Deadlock Advisory</h3>
 * Operations against a {@code SovereignDataset} rely on internal, record-level locks to ensure operations
 * against any given {@code Record} do not interfere with each other.  The number of record locks available
 * is based on the value supplied to the
 * {@link com.terracottatech.sovereign.impl.SovereignBuilder#concurrency SovereignBuilder.concurrency}
 * method.  Selection of a lock for access to a {@code Record} is based on a hash of the key value;
 * multiple records will share a single lock.
 * <p>
 * The use of synchronization within functions or predicates supplied to {@code SovereignDataset}
 * methods can result in a deadlock.  To avoid such deadlocks, any attempt to acquire a record lock that
 * exceeds the duration set by
 * {@link com.terracottatech.sovereign.impl.SovereignBuilder#recordLockTimeout SovereignBuilder.recordLockTimeout}
 * will be terminated using a
 * {@link com.terracottatech.sovereign.exceptions.LockConflictException LockConflictException}
 * resulting in failure of at least one dataset operation.
 *
 * @param <K> the dataset key type
 *
 * @author Alex Snaps
 *
 * @see com.terracottatech.store.Dataset
 */
@SuppressWarnings("JavadocReference")
public interface SovereignDataset<K extends Comparable<K>> {

  /**
   * Returns the type of the keys used in this {@code SovereignDataset}.
   *
   * @return the dataset key type
   */
  Type<K> getType();

  /**
   * Returns the {@link TimeReferenceGenerator} instance used for this {@code SovereignDataset}.
   *
   * @return the {@code TimeReferenceGenerator} instance
   */
  TimeReferenceGenerator<? extends TimeReference<?>> getTimeReferenceGenerator();

  /**
   * Adds a new {@link Record} to this {@code SovereignDataset} if, and only if, a {@code Record} does
   * not already exist for the key.  The new {@code Record} is composed of the key and cells provided.
   *
   * @param durability the {@code Durability} level under which the {@code Record} added is persisted
   * @param key the value of the key under which the {@code Record} is added
   * @param cells one or more {@code Cell} instances used to compose the new {@code Record}
   *
   * @return {@code null} if a new {@code Record} was successfully added; a copy of the existing
   *      {@code Record} if {@code key} exists in this {@code SovereignDataset}
   *
   * @see com.terracottatech.store.DatasetWriterReader#add(Comparable, Cell[])
   */
  default SovereignRecord<K> add(Durability durability, K key, Cell<?>... cells) {
    return add(durability, key, asList(cells));
  }

  /**
   * Adds a new {@link Record} to this {@code SovereignDataset} if, and only if, a {@code Record} does
   * not already exist for the key.  The new {@code Record} is composed of the key and cells from the
   * supplied {@code Iterable}.
   *
   * @param durability the {@code Durability} level under which the {@code Record} added is persisted
   * @param key the value of the key under which the {@code Record} is added
   * @param cells a non-{@code null} {@code Iterable} providing one or more {@code Cell} instances used to compose
   *              the new {@code Record}
   *
   * @return {@code null} if a new {@code Record} was successfully added; a copy of the existing
   *      {@code Record} if {@code key} exists in this {@code SovereignDataset}
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   *
   * @see com.terracottatech.store.DatasetWriterReader#add(Comparable, Iterable)
   */
  SovereignRecord<K> add(Durability durability, K key, Iterable<Cell<?>> cells);

  /**
   * Changes an existing {@link Record} identified by its key using replacement {@link Cell}s produced by a
   * {@link Function} against the {@code Record}.  If a {@code Record} for the key does not exist in this
   * {@code SovereignDataset}, the dataset is not altered.
   * <p/>
   * This operation is equivalent to:
   * <pre>
   *   {@link #applyMutation(Durability, Comparable, Predicate, Function, BiFunction) applyMutation(durability, key, transform, (rIn, rOut) -&gt; Optional.empty())}
   * </pre>
   *
   * @param durability the {@code Durability} level under which the modification of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to modify; if a {@code Record} for {@code key} does
   *            not exist, the dataset is not changed
   * @param predicate {@code Predicate} which tests the {@code Record} to decide if the mutation should be applied.
   *              If false, the {@code Record} will not be mutated.
   * @param transform the {@code Function}, taking a {@code Record} and returning an {@link Iterable} over
   *                  replacement {@link Cell}s, used to provide {@code Cell} values to compose the
   *                  new {@code Record};  any {@code Cell} for which a replacement value is not provided by
   *                  {@code transform} is removed from the new {@code Record}.
   *                  This function is not invoked if a {@code Record} for {@code key} is not found.
   *
   * @see com.terracottatech.store.ConditionalReadWriteRecordAccessor#update(UpdateOperation)
   */
  void applyMutation(Durability durability,
                     K key,
                     Predicate<? super Record<K>> predicate,
                     Function<? super Record<K>, Iterable<Cell<?>>> transform);

  /**
   * Changes an existing {@link Record} identified by its key using replacement {@link Cell}s produced by a
   * {@link Function} against the {@code Record}.  The return value of this method is determined by the application
   * of a {@link BiFunction} on the old and new {@code Record}s.  If a {@code Record} for the key does not exist in
   * this {@code SovereignDataset}, the dataset is not altered.
   *
   * @param <T> the return type of the {@code output} {@link BiFunction}
   *
   * @param durability the {@code Durability} level under which the modification of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to modify; if a {@code Record} for {@code key} does
   *            not exist, the dataset is not changed and {@code Optional.empty()} is returned
   * @param predicate {@code Predicate} which tests the {@code Record} to decide if the mutation should be applied.
   *          If false, an {@code Optional.empty()} will occur in the stream instead of the {@code Record}.
   * @param transform the {@code Function}, taking a {@code Record} and returning an {@link Iterable} over
   *                  replacement {@link Cell}s, used to provide {@code Cell} values to compose the
   *                  new {@code Record};  any {@code Cell} for which a replacement value is not provided by
   *                  {@code transform} is removed from the new {@code Record}.
   *                  This function is not invoked if a {@code Record} for {@code key} is not found.
   * @param output a {@code BiFunction} taking the original {@code Record} and the new, updated {@code Record} (formed
   *               from the {@code Cell}s provided by {@code transform}) used to generate the result of this
   *               {@code applyMutation} method; this function is not invoked if a {@code Record} for {@code key}
   *               is not found
   *
   * @return {@code Optional.empty()} if a {@code Record} for {@code key} does not exist in this {@code SovereignDataset};
   *      <code>Optional.of({@link BiFunction#apply output.apply(oldRecord, newRecord)})</code> otherwise
   *
   * @see com.terracottatech.store.ConditionalReadWriteRecordAccessor#update(UpdateOperation, BiFunction)
   */
  <T> Optional<T> applyMutation(Durability durability,
                                K key,
                                Predicate<? super Record<K>> predicate,
                                Function<? super Record<K>, Iterable<Cell<?>>> transform,
                                BiFunction<? super Record<K>, ? super Record<K>, T> output);

  /**
   * Provides a {@link Consumer} that modifies, in this {@code SovereignDataset}, each {@link Record}
   * presented to the consumer.
   *
   * @param durability the {@code Durability} level under which the modification of each {@code Record} is persisted
   * @param transform the {@code Function}, taking a {@code Record} and returning an {@link Iterable} over
   *                  replacement {@link Cell}s, used to provide {@code Cell} values to compose the
   *                  new {@code Record};  any {@code Cell} for which a replacement value is not provided by
   *                  {@code transform} is removed from the new {@code Record}.
   * @return a new {@code Consumer} through which {@code Record}s in this dataset may be modified
   */
  Consumer<Record<K>> applyMutation(Durability durability, Function<? super Record<K>, Iterable<Cell<?>>> transform);

  /**
   * Provides a {@link Function} that modifies, in this {@code SovereignDataset}, each {@link Record}
   * presented to the function.  The return values produced by the {@code Function} are determined by
   * the application of a {@link BiFunction} on each pair of old and new {@code Record}s.
   *
   * @param <T> the return type of the {@code output} {@link BiFunction}
   *
   * @param durability the {@code Durability} level under which the modification of each {@code Record} is persisted
   * @param transform the {@code Function}, taking a {@code Record} and returning an {@link Iterable} over
   *                  replacement {@link Cell}s, used to provide new {@code Cell} values to compose the
   *                  new {@code Record};  any {@code Cell} for which a replacement value is not provided by
   *                  {@code transform} is removed from the new {@code Record}.
   * @param output a {@code BiFunction} taking the original {@code Record} and the new, updated {@code Record} (formed
   *               from the {@code Cell}s provided by {@code transform}) used to generate the result of this
   *               {@code applyMutation} method
   *
   * @return a new {@code Function} through which {@code Record}s in this dataset may be modified
   */
  <T> Function<Record<K>, T> applyMutation(Durability durability, Function<? super Record<K>, Iterable<Cell<?>>> transform,
                                           BiFunction<? super Record<K>, ? super Record<K>, T> output);

  /**
   * Provides a {@link Consumer} that deletes, from this {@code SovereignDataset}, each {@link Record}
   * presented to the consumer.
   *
   * @param durability the {@code Durability} level under which the deletion of each {@code Record} is persisted
   *
   * @return a new {@code Consumer} through which {@code Record}s may be deleted from this dataset
   */
  Consumer<Record<K>> delete(Durability durability);

  /**
   * Provides a {@link Function} that deletes, from this {@code SovereignDataset}, each {@link Record}
   * presented to the function.  The return values produced by the {@code Function} are determined by
   * the application of a {@code Function} on the deleted {@code Record}s.
   *
   * @param <T> the return type of the {@code output} {@link BiFunction}
   *
   * @param durability the {@code Durability} level under which the modification of each {@code Record} is persisted
   * @param output a {@code Function} taking the deleted {@code Record} and producing the result of the
   *               {@code Function} returned by this method
   *
   * @return a new {@code Function} through which {@code Record}s may be deleted from this dataset
   */
  <T> Function<Record<K>, T> delete(Durability durability, Function<? super Record<K>, T> output);

  /**
   * Deletes the {@link Record} identified by the key provided from this {@code SovereignDataset}.
   *
   * @param durability the {@code Durability} level under which the deletion of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to delete
   *
   * @return a copy of the {@code Record} deleted; {@code null} if no {@code Record} for {@code key} exists
   *      in this {@code SovereignDataset}
   *
   * @see com.terracottatech.store.DatasetWriterReader#delete(Comparable)
   */
  SovereignRecord<K> delete(Durability durability, K key);

  /**
   * Deletes the {@link Record} identified by the key provided from this {@code SovereignDataset} if, and
   * only if, the {@link Predicate} provided returns {@code true}.
   *
   * @param durability the {@code Durability} level under which the deletion of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to delete
   * @param predicate a {@code Predicate} to test against the {@code Record} for {@code key}; if {@code predicate}
   *                  returns {@code true}, the {@code Record} is deleted
   *
   * @return a copy of the {@code Record} deleted; {@code null} if no {@code Record} was deleted (either because
   *      a {@code Record} did not exist for {@code key} or {@code predicate} returned {@code false}
   *
   * @see ConditionalReadWriteRecordAccessor#delete()
   */
  SovereignRecord<K> delete(Durability durability,
                            K key,
                            Predicate<? super Record<K>> predicate);

  /**
   * Gets the {@link Record} stored in this {@code SovereignDataset} under the key provided.
   *
   * @param key the value of the key of the {@code Record} to fetch
   *
   * @return a copy of the {@code Record} for {@code key}; {@code null} if there is no {@code Record}
   *      for {@code key} in this dataset
   *
   * @see com.terracottatech.store.DatasetReader#get
   */
  SovereignRecord<K> get(K key);

  /**
   * Gets the {@link Record} stored in this {@code SovereignDataset} under the key provided.
   *
   * @param key the value of the key of the {@code Record} to fetch
   *
   * @return a copy of the {@code Record} for {@code key}; {@code null} if there is no {@code Record}
   *      for {@code key} in this dataset
   *
   * @throws RecordLockedException if a stable view of the target record cannot be obtained and a lock
   *        attempted to establish a stable view fails
   * @see com.terracottatech.store.DatasetReader#get
   */
  SovereignRecord<K> tryGet(K key) throws RecordLockedException;

  /**
   * Adds a new {@link Record} to this {@code SovereignDataset} if, and only if, a {@code Record} does
   * not already exist for the key and the lock for the key is immediately available.  The new {@code Record} is
   * composed of the key and cells from the supplied {@code Iterable}.
   *
   * @param durability the {@code Durability} level under which the {@code Record} added is persisted
   * @param key the value of the key under which the {@code Record} is added
   * @param cells a non-{@code null} {@code Iterable} providing one or more {@code Cell} instances used to compose
   *              the new {@code Record}
   *
   * @return {@code null} if a new {@code Record} was successfully added; a copy of the existing
   *      {@code Record} if {@code key} exists in this {@code SovereignDataset}
   *
   * @throws NullPointerException if {@code cells} is {@code null}
   * @throws RecordLockedException if the lock for the target key is not immediately available
   *
   * @see com.terracottatech.store.DatasetWriterReader#add(Comparable, Iterable)
   */
  SovereignRecord<K> tryAdd(Durability durability, K key, Iterable<Cell<?>> cells) throws RecordLockedException;

  /**
   * Adds a new {@link Record} to this {@code SovereignDataset} if, and only if, a {@code Record} does
   * not already exist for the key and the lock for the key is immediately available.  The new {@code Record} is
   * composed of the key and cells provided.
   *
   * @param durability the {@code Durability} level under which the {@code Record} added is persisted
   * @param key the value of the key under which the {@code Record} is added
   * @param cells one or more {@code Cell} instances used to compose the new {@code Record}
   *
   * @return {@code null} if a new {@code Record} was successfully added; a copy of the existing
   *      {@code Record} if {@code key} exists in this {@code SovereignDataset}
   *
   * @throws RecordLockedException if the lock for the target key is not immediately available
   *
   * @see com.terracottatech.store.DatasetWriterReader#add(Comparable, Cell[])
   */
  default SovereignRecord<K> tryAdd(Durability durability, K key, Cell<?>... cells) throws RecordLockedException {
    return tryAdd(durability, key, asList(cells));
  }

  /**
   * Changes an existing {@link Record} identified by its key using replacement {@link Cell}s produced by a
   * {@link Function} against the {@code Record} if the lock for the key is immediately available.  If a
   * {@code Record} for the key does not exist in this {@code SovereignDataset}, the dataset is not altered.
   * <p/>
   * This operation is equivalent to:
   * <pre>
   *   {@link #applyMutation(Durability, Comparable, Predicate, Function, BiFunction) applyMutation(durability, key, transform, (rIn, rOut) -&gt; Optional.empty())}
   * </pre>
   *
   * @param durability the {@code Durability} level under which the modification of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to modify; if a {@code Record} for {@code key} does
   *            not exist, the dataset is not changed
   * @param predicate {@code Predicate} which tests the {@code Record} to decide if the mutation should be applied.
   *              If false, the {@code Record} will not be mutated.
   * @param transform the {@code Function}, taking a {@code Record} and returning an {@link Iterable} over
   *                  replacement {@link Cell}s, used to provide {@code Cell} values to compose the
   *                  new {@code Record};  any {@code Cell} for which a replacement value is not provided by
   *                  {@code transform} is removed from the new {@code Record}.
   *                  This function is not invoked if a {@code Record} for {@code key} is not found.
   *
   * @throws RecordLockedException if the lock for the target key is not immediately available
   *
   * @see com.terracottatech.store.ConditionalReadWriteRecordAccessor#update(UpdateOperation)
   */
  void tryApplyMutation(Durability durability,
                        K key,
                        Predicate<? super Record<K>> predicate,
                        Function<? super Record<K>, Iterable<Cell<?>>> transform)
      throws RecordLockedException;

  /**
   * Changes an existing {@link Record} identified by its key using replacement {@link Cell}s produced by a
   * {@link Function} against the {@code Record} if the lock for the key is immediately available.  The return
   * value of this method is determined by the application of a {@link BiFunction} on the old and new
   * {@code Record}s.  If a {@code Record} for the key does not exist in this {@code SovereignDataset}, the
   * dataset is not altered.
   *
   * @param <T> the return type of the {@code output} {@link BiFunction}
   *
   * @param durability the {@code Durability} level under which the modification of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to modify; if a {@code Record} for {@code key} does
   *            not exist, the dataset is not changed and {@code Optional.empty()} is returned
   * @param predicate {@code Predicate} which tests the {@code Record} to decide if the mutation should be applied.
   *          If false, an {@code Optional.empty()} will occur in the stream instead of the {@code Record}.
   * @param transform the {@code Function}, taking a {@code Record} and returning an {@link Iterable} over
   *                  replacement {@link Cell}s, used to provide {@code Cell} values to compose the
   *                  new {@code Record};  any {@code Cell} for which a replacement value is not provided by
   *                  {@code transform} is removed from the new {@code Record}.
   *                  This function is not invoked if a {@code Record} for {@code key} is not found.
   * @param output a {@code BiFunction} taking the original {@code Record} and the new, updated {@code Record} (formed
   *               from the {@code Cell}s provided by {@code transform}) used to generate the result of this
   *               {@code applyMutation} method; this function is not invoked if a {@code Record} for {@code key}
   *               is not found
   *
   * @return {@code Optional.empty()} if a {@code Record} for {@code key} does not exist in this {@code SovereignDataset};
   *      <code>Optional.of({@link BiFunction#apply output.apply(oldRecord, newRecord)})</code> otherwise
   *
   * @throws RecordLockedException if the lock for the target key is not immediately available
   *
   * @see com.terracottatech.store.ConditionalReadWriteRecordAccessor#update(UpdateOperation, BiFunction)
   */
  <T> Optional<T> tryApplyMutation(Durability durability,
                                   K key,
                                   Predicate<? super Record<K>> predicate,
                                   Function<? super Record<K>, Iterable<Cell<?>>> transform,
                                   BiFunction<? super Record<K>, ? super Record<K>, T> output)
      throws RecordLockedException;

  /**
   * Deletes the {@link Record} identified by the key provided from this {@code SovereignDataset} if
   * the lock for the key is immediately available.
   *
   * @param durability the {@code Durability} level under which the deletion of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to delete
   *
   * @return a copy of the {@code Record} deleted; {@code null} if no {@code Record} for {@code key} exists
   *      in this {@code SovereignDataset}
   *
   * @throws RecordLockedException if the lock for the target key is not immediately available
   *
   * @see com.terracottatech.store.DatasetWriterReader#delete(Comparable)
   */
  SovereignRecord<K> tryDelete(Durability durability, K key) throws RecordLockedException;

  /**
   * Deletes the {@link Record} identified by the key provided from this {@code SovereignDataset} if, and
   * only if, the {@link Predicate} provided returns {@code true} and the lock for the key is immediately
   * available.
   *
   * @param durability the {@code Durability} level under which the deletion of the {@code Record} is persisted
   * @param key the value of the key of the {@code Record} to delete
   * @param predicate a {@code Predicate} to test against the {@code Record} for {@code key}; if {@code predicate}
   *                  returns {@code true}, the {@code Record} is deleted
   *
   * @return a copy of the {@code Record} deleted; {@code null} if no {@code Record} was deleted (either because
   *      a {@code Record} did not exist for {@code key} or {@code predicate} returned {@code false}
   *
   * @throws RecordLockedException if the lock for the target key is not immediately available
   *
   * @see ConditionalReadWriteRecordAccessor#delete()
   */
  SovereignRecord<K> tryDelete(Durability durability, K key, Predicate<? super Record<K>> predicate) throws RecordLockedException;

  /**
   * Produces a {@link Stream} over the {@link Record}s contained in this {@code SovereignDataset}.
   *
   * @return a new {@code Stream} instance
   *
   * @see DatasetReader#records()
   */
  RecordStream<K> records();

  /**
   * Gets the {@link SovereignIndexing} instance through which secondary indexes defined for
   * this {@code SovereignDataset} may be managed.
   *
   * @return the {@code SovereignIndexing} instance for this {@code SovereignDataset}
   */
  SovereignIndexing getIndexing();

  /**
   * Upon return, all prior operations which were
   * made with durability {@link com.terracottatech.sovereign.SovereignDataset.Durability.LAZY}
   * will persisted, if the dataset supports persistence.
   **/
  void flush();

  /**
   * Indicates if this {@code Dataset} is undergoing disposal or is presently disposed and no longer usable.
   *
   * @return {@code true} if the {@link #dispose} method has been called whether or not the disposal
   *    operation is complete; {@code false} otherwise
   */
  boolean isDisposed();

  /**
   * Return generated UUID for this dataset.
   *
   * @return generated UUID
   */
  UUID getUUID();

  /**
   * Return the description for this dataset. This is an opaque snapshot of the state of the
   * dataset, minus data.
   *
   * @return dataset description
   */
  SovereignDatasetDescription getDescription();

  /**
   * Get the named alias for this dataset. Does not have to be unique. Simply along for the ride.
   *
   * @return String alias
   */
  String getAlias();

    /**
     * Identifies the level of persistence durability in effect for a {@link SovereignDataset} mutation operation.
     * For {@code SovereignDataset} mutation operations returning a {@link Function} or {@link Consumer}, the
     * durability setting applies to each invocation of the function or consumer.
     */
  enum Durability {
    /**
     * Indicates a {@code SovereignDataset} mutation operation does not wait for a {@code Record} mutation
     * to be persisted before the mutation method returns.  Mutations are recorded in the persistence layer
     * in a deferred manner and are synchronized with the persistence layer before or concurrent with the
     * next mutation specifying {@link #IMMEDIATE}.  Failures in deferred persistence operations cause
     * the first {@code SovereignDataset} data access operation following recognition of the failure to
     * be terminated by a
     * {@link com.terracottatech.sovereign.exceptions.PersistenceFailureException PersistenceFailureException}
     * describing the failure.
     */
    LAZY,

      /**
     * Indicates a {@code SovereignDataset} mutation operation waits for a {@code Record} mutation to be
     * persisted before the mutation method returns.  Persistence failures are recognized immediately
     * and the operation terminated by a
     * {@link com.terracottatech.sovereign.exceptions.PersistenceFailureException PersistenceFailureException}
     * describing the failure.
     */
    IMMEDIATE
  }

  /**
   * Get the dataset schema object.
   * @return
   */
  DatasetSchema getSchema();

  /**
   * Return storage implementation this dataset belongs to.
   */
  SovereignStorage<?, ?> getStorage();

  /**
   * If this dataset encountered an extinction situation, this retrieves
   * the exception that caused it.
   * @return
   */
  SovereignExtinctionException getExtinctionException();

  /**
   * Get the amount of memory allocated to store the main heap of records.
   * @return bytes
   */
  long getAllocatedHeapStorageSize();

  /**
   * Get the amount of memory occupied by the main heap of records.
   * @return bytes
   */
  long getOccupiedHeapStorageSize();

  /**
   * Get the amount of memory allocated to store the main K::Record hash association
   * @return bytes
   */
  long getAllocatedPrimaryKeyStorageSize();

  /**
   * Get the amount of memory in use to store the main K::Record hash association
   * @return bytes
   */
  long getOccupiedPrimaryKeyStorageSize();

  /**
   * Get the number of persistent bytes used by this dataset.
   * @return bytes
   */
  long getPersistentBytesUsed();

  /**
   * get allocated amount of memory used in support of persistence.
   * @return
   */
  long getAllocatedPersistentSupportStorage();

  /**
   * get occupied amount of memory used in support of persistence.
   * @return
   */
  long getOccupiedPersistentSupportStorage();

  /**
   * Get the number of bytes allocated for all the indexes in the dataset.
   * @return bytes
   */
  long getAllocatedIndexStorageSize();

  /**
   * Formatted string containing all stats of this dataset. The line separator used is the platform one.
   * @return formatted string
   */

  default String getStatsDump() {
    String lineSeparator = System.lineSeparator();
    StringBuilder sb = new StringBuilder(2048); // Rough estimation of the capacity
    sb.append("Memory usage:");
    sb.append(lineSeparator);

    sb.append("\tHeap Storage:");
    sb.append(lineSeparator);

    sb.append("\t\tOccupied: ");
    sb.append(MiscUtils.bytesAsNiceString(getOccupiedHeapStorageSize()));
    sb.append(lineSeparator);

    sb.append("\t\tAllocated: ");
    sb.append(MiscUtils.bytesAsNiceString(getAllocatedHeapStorageSize()));
    sb.append(lineSeparator);

    sb.append("\tPersistent Support Storage:");
    sb.append(lineSeparator);

    sb.append("\t\tOccupied: ");
    sb.append(MiscUtils.bytesAsNiceString(getOccupiedPersistentSupportStorage()));
    sb.append(lineSeparator);

    sb.append("\t\tAllocated: ");
    sb.append(MiscUtils.bytesAsNiceString(getAllocatedPersistentSupportStorage()));
    sb.append(lineSeparator);

    sb.append("\tPrimary Key Storage:");
    sb.append(lineSeparator);

    sb.append("\t\tOccupied: ");
    sb.append(MiscUtils.bytesAsNiceString(getOccupiedPrimaryKeyStorageSize()));
    sb.append(lineSeparator);

    sb.append("\t\tAllocated: ");
    sb.append(MiscUtils.bytesAsNiceString(getAllocatedPrimaryKeyStorageSize()));
    sb.append(lineSeparator);

    sb.append("\tIndex Storage:");
    sb.append(lineSeparator);

    sb.append("\t\tAllocated: ");
    sb.append(MiscUtils.bytesAsNiceString(getAllocatedIndexStorageSize()));
    sb.append(lineSeparator);

    sb.append("\tTotal Persistent Size: ");
    sb.append(MiscUtils.bytesAsNiceString(getPersistentBytesUsed()));
    sb.append(lineSeparator);

    sb.append("Index details:");
    sb.append(lineSeparator);

    List<SovereignIndex<?>> indexes = getIndexing().getIndexes();
    if (indexes.isEmpty()) {
      sb.append("\tN/A");
    } else {
      for (SovereignIndex<?> si : indexes) {
        SovereignIndexStatistics stats = si.getStatistics();

        sb.append("\t");
        sb.append(si.getDescription().getCellName());
        sb.append(":");
        sb.append(lineSeparator);

        sb.append("\t\tIndexed Record Count: ");
        sb.append(stats.indexedRecordCount());
        sb.append(lineSeparator);

        sb.append("\t\tOccupied Storage: ");
        sb.append(MiscUtils.bytesAsNiceString(stats.occupiedStorageSize()));
        sb.append(lineSeparator);

        sb.append("\t\tAccess Count: ");
        sb.append(stats.indexAccessCount());
        sb.append(lineSeparator);
      }
    }

    return sb.toString();
  }


  /**
   * Get number of records.
   * @return
   */
  long recordCount();
}
