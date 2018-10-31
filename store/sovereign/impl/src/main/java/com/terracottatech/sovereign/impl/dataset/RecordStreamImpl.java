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

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.sovereign.impl.compute.CellComparison;
import com.terracottatech.sovereign.plan.StreamPlan;
import com.terracottatech.store.common.dataset.stream.PipelineMetaData;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.Operation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.OperationMetaData;
import com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation;
import com.terracottatech.store.common.dataset.stream.WrappedReferenceStream;
import com.terracottatech.store.common.dataset.stream.WrappedStream;
import com.terracottatech.store.Record;
import com.terracottatech.store.internal.InternalRecord;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * A {@link Stream} implementation supporting processing of {@link Record} instances
 * from a {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset}.
 * <p>
 * {@code RecordStream} extends the operations available to {@link Stream}
 * by adding a {@link #selfClose(boolean)} operation.  JDK-provided {@code Stream}
 * instances, with some exceptions, do <b>not</b> close on termination -- even when
 * the stream is exhausted.  With the exception of the {@link #iterator()} and
 * {@link #spliterator()} methods, a {@code RecordStream} has the capability of
 * self-closing after completion of terminal operations.  Whether or not
 * the stream self-closes is determined by the last use of the {@code selfClose}
 * operation in the stream.  By default, {@code selfClose} is set to
 * <code>{@value WrappedStream#SELF_CLOSE_DEFAULT}</code>.
 * If self-close is not enabled, streams based on {@code RecordStream} must be
 * explicitly closed to ensure proper resource disposal.  This may be done either by
 * calling {@link Stream#close()} directly or using try-with-resources.
 * The following is a recommended usage pattern:
 * <pre>{@code
 * SovereignDataset dataset = ...
 * List<Animals.Animal> mammals;
 * try (Stream<Record<String>> base = dataset.records()) {
 *   mammals = base
 *       .filter(compare(Animals.Schema.TAXONOMIC_CLASS).is("mammal"))
 *       .map(Animals.Animal::new)
 *       .collect(toList());
 * }
 * }</pre>
 * Closure of any {@code Stream} in the stream pipeline closes the full pipeline so the
 * following alternative also works:
 * <pre>{@code
 * SovereignDataset dataset = ...
 * List<Animals.Animal> mammals;
 * try (Stream<Animals.Animal> mapStream = dataset.records()
 *          .filter(compare(Animals.Schema.TAXONOMIC_CLASS).is("mammal"))
 *          .map(Animals.Animal::new)) {
 *   mammals = mapStream.collect(toList());
 * }
 * }</pre>
 *
 * @author mscott
 * @author Clifford W. Johnson
 */
public final class RecordStreamImpl<K extends Comparable<K>>
    extends WrappedReferenceStream<Record<K>> implements RecordStream<K> {
  private final LocatorGenerator<K>  generator;

  /**
   * Private constructor; use {@link #newInstance(Catalog, boolean)} to obtain a new
   * {@code RecordStream} instance.
   *  @param nativeStream the Java {@code Stream} implementation on which this
   *                     {@code RecordStream} is based
   * @param dataset the {@code Catalog} from which the stream content is obtained
   */
  private RecordStreamImpl(Stream<Record<K>> nativeStream, Catalog<K> dataset) {
    super(nativeStream);
    this.generator = new LocatorGenerator<>(dataset);
  }

  /**
   * Private constructor used by {@link #wrap(Stream)} to form a new, chainable {@code RecordStream}.
   *
   * @param generator the {@code LocatorGenerator} from the original {@code RecordStream}
   * @param wrappedStream the new Java {@code Stream} to wrap
   */
  private RecordStreamImpl(final LocatorGenerator<K> generator,
                           final Stream<Record<K>> wrappedStream) {
    super(wrappedStream, false);
    this.generator = generator;
  }

  @Override
  protected final WrappedReferenceStream<Record<K>> wrap(final Stream<Record<K>> stream) {
    return new RecordStreamImpl<>(this.generator, stream);
  }

  /**
   * Creates a new {@code RecordStream} based on the {@link Catalog} provided.
   * The {@link Spliterator} on which the stream is based is not created until the
   * stream pipeline terminal operation is invoked.
   *
   * @param catalog the {@code Catalog} instance on which the {@code Stream} is based
   * @param parallel if {@code true}, the stream is enabled for parallel processing;
   *                 if {@code false}, the stream is enabled for sequential processing
   * @param <K> the key type of the {@link Record} instances from {@code catalog}
   *
   * @return a new {@code RecordStream}
   */
  public static <K extends Comparable<K>> RecordStream<K> newInstance(
    final Catalog<K> catalog, final boolean parallel) {

    final SpliteratorSource<K> spliteratorSource = new SpliteratorSource<>();
    final RecordStreamImpl<K> stream = new RecordStreamImpl<>(
        StreamSupport.stream(spliteratorSource, Spliterator.NONNULL | Spliterator.DISTINCT, parallel), catalog);
    spliteratorSource.setRecordStream(stream);
    stream.appendTerminalAction(spliteratorSource);

    return stream;
  }

  /**
   * {@inheritDoc}
   * <p>This implementation adds {@code distinct} to the wrapped stream then ends the current stream
   * pipeline by
   * <ol>
   *   <li>taking a {@code Spliterator} on the current stream</li>
   *   <li>wrapping that spliterator in a {@link PassThroughManagedSpliterator}</li>
   *   <li>and forming a new {@code RecordStream} using the pass-through spliterator as its data source.</li>
   * </ol>
   * The {@code PassThroughManagedSpliterator} instance is a potential insertion point for a
   * {@link ManagedAction} instance used later in the pipeline.
   *
   * @return {@inheritDoc}
   */
  // See TDB-1350 for distinct() processing details
  @Override
  public final Stream<Record<K>> distinct() {
    return chainWithInsertionPoint(IntermediateOperation.DISTINCT, nativeStream.distinct(),
        Spliterator.NONNULL | Spliterator.DISTINCT);
  }

  /**
   * {@inheritDoc}
   * <p>This implementation adds {@code limit} to the wrapped stream then ends the current stream
   * pipeline by
   * <ol>
   *   <li>taking a {@code Spliterator} on the current stream</li>
   *   <li>wrapping that spliterator in a {@link PassThroughManagedSpliterator}</li>
   *   <li>and forming a new {@code RecordStream} using the pass-through spliterator as its data source.</li>
   * </ol>
   * The {@code PassThroughManagedSpliterator} instance is a potential insertion point for a
   * {@link ManagedAction} instance used later in the pipeline.
   *
   * @param maxSize {@inheritDoc}
   * @return {@inheritDoc}
   */
  // See TDB-1350 for limit() processing details
  @Override
  public final Stream<Record<K>> limit(final long maxSize) {
    return chainWithInsertionPoint(IntermediateOperation.LIMIT, nativeStream.limit(maxSize),
        Spliterator.NONNULL | Spliterator.DISTINCT, maxSize);
  }

  /**
   * {@inheritDoc}
   * <p>This implementation disallows an instance of {@link ManagedAction} as {@code action}.
   *
   * @param action {@inheritDoc}
   * @return {@inheritDoc}
   *
   * @throws IllegalArgumentException if {@code action} is a {@code ManagedAction}
   */
  @Override
  public final Stream<Record<K>> peek(final Consumer<? super Record<K>> action) {
    if (action instanceof ManagedAction) {
      throw new IllegalArgumentException("peek argument may not be a ManagedAction");
    }
    return super.peek(action);
  }

  /**
   * {@inheritDoc}
   * <p>This implementation adds {@code skip} to the wrapped stream then ends the current stream
   * pipeline by
   * <ol>
   *   <li>taking a {@code Spliterator} on the current stream</li>
   *   <li>wrapping that spliterator in a {@link PassThroughManagedSpliterator}</li>
   *   <li>and forming a new {@code RecordStream} using the pass-through spliterator as its data source.</li>
   * </ol>
   * The {@code PassThroughManagedSpliterator} instance is a potential insertion point for a
   * {@link ManagedAction} instance used later in the pipeline.
   *
   * @param n {@inheritDoc}
   * @return {@inheritDoc}
   */
  // See TDB-1350 for skip() processing details
  @Override
  public final Stream<Record<K>> skip(final long n) {
    return chainWithInsertionPoint(IntermediateOperation.SKIP, nativeStream.skip(n),
        Spliterator.NONNULL | Spliterator.DISTINCT, n);
  }

  /**
   * {@inheritDoc}
   * <p>This implementation adds {@code sorted} to the wrapped stream then ends the current stream
   * pipeline by
   * <ol>
   *   <li>taking a {@code Spliterator} on the current stream</li>
   *   <li>wrapping that spliterator in a {@link PassThroughManagedSpliterator}</li>
   *   <li>and forming a new {@code RecordStream} using the pass-through spliterator as its data source.</li>
   * </ol>
   * The {@code PassThroughManagedSpliterator} instance is a potential insertion point for a
   * {@link ManagedAction} instance used later in the pipeline.
   *
   * @return {@inheritDoc}
   */
  @Override
  public final Stream<Record<K>> sorted() {
    return chainWithInsertionPoint(IntermediateOperation.SORTED_0, nativeStream.sorted(),
        Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.SORTED);
  }

  /**
   * {@inheritDoc}
   * <p>This implementation adds {@code sorted} to the wrapped stream then ends the current stream
   * pipeline by
   * <ol>
   *   <li>taking a {@code Spliterator} on the current stream</li>
   *   <li>wrapping that spliterator in a {@link PassThroughManagedSpliterator}</li>
   *   <li>and forming a new {@code RecordStream} using the pass-through spliterator as its data source.</li>
   * </ol>
   * The {@code PassThroughManagedSpliterator} instance is a potential insertion point for a
   * {@link ManagedAction} instance used later in the pipeline.
   *
   * @param comparator {@inheritDoc}
   *
   * @return {@inheritDoc}
   */
  @Override
  public final Stream<Record<K>> sorted(final Comparator<? super Record<K>> comparator) {
    requireNonNull(comparator, "comparator");
    return chainWithInsertionPoint(IntermediateOperation.SORTED_1, nativeStream.sorted(comparator),
        Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.SORTED, comparator);
  }

  /**
   * Forms a new stream pipeline with a {@code ManagedAction} insertion point using a {@code Spliterator}
   * taken from the original (native) stream as the new stream's source.  The spliterator taken on the
   * original stream is wrapped in a {@link PassThroughManagedSpliterator} which serves as the insertion
   * point for the {@code ManagedAction} instance.
   * <p>This method is used to add an operation to a pipeline where that operation is capable of affecting
   * element delivery order.  These are stateful, intermediate operations such as {@code sorted} and
   * {@code skip} that can hold, drop, or reorder stream elements.  The locking capabilities of a
   * {@code ManagedAction}, which may be present in operations like {@code forEach} and {@code map}, are
   * typically carried out in the stream's source {@code Spliterator}.  Unfortunately, performing lock
   * management in the source spliterator is not effective for pipelines with operations affecting
   * element deliver order.  This method is used to enable movement of lock management to follow any such
   * operations.
   * <p>The {@link WrappedStream#getMetaData() operation meta data} reflects only {@code operation} and
   * not the presence of the added {@code spliterator} operation or the new stream.
   *
   * @implNote The characteristics for the {@link PassThroughManagedSpliterator} must be provided
   *    ({@code characteristics}) to avoid premature allocation of the stream's source {@code Spliterator}.
   *
   * @param operation identification of the {@code Stream} operation being appended
   * @param nativeStream the Java {@code Stream} reference to the pipeline <i>after</i> the operation is
   *                     appended; a {@code Spliterator} will be taken from this stream
   * @param characteristics characteristics to apply to the {@link PassThroughManagedSpliterator} used
   *                        to wrap the spliterator taken from {@code nativeStream}.
   * @param operationArguments the arguments provided to the {@code Stream} operation
   *
   * @return a new {@code RecordStream}
   */
  private Stream<Record<K>> chainWithInsertionPoint(
      final IntermediateOperation operation, final Stream<Record<K>> nativeStream,
      final int characteristics, final Object... operationArguments) {

    /*
     * Form a new stream pipeline using the original pipeline (ending with the sorted operation)
     * as the source of a Spliterator feeding the new pipeline.  The Spliterator feeding the new
     * pipeline may use the ManagedAction, if any, to providing record locking.  (A Spliterator
     * Supplier is used to prevent a premature reference to the source [sorted] Spliterator.)
     */
    final ManagedActionSupplier<K> managedActionSupplier = new ManagedActionSupplier<>();
    final PassThroughManagedSpliterator<K> passThroughSpliterator =
        new PassThroughManagedSpliterator<>(nativeStream.spliterator(), managedActionSupplier);
    Stream<Record<K>> downstreamNativeStream =
        StreamSupport.stream(() -> passThroughSpliterator, characteristics, isParallel());
    WrappedReferenceStream<Record<K>> managedStream = wrap(downstreamNativeStream);

    /*
     * Interlink closure of the original and new stream pipelines.
     */
    associateCloseable(passThroughSpliterator);
    associateCloseable(downstreamNativeStream);
    downstreamNativeStream.onClose(this::close);

    return chain(operation, () -> managedStream, managedActionSupplier, operationArguments);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RecordStream<K> explain(Consumer<? super StreamPlan> consumer) {
    return (RecordStream<K>) chain(IntermediateOperation.EXPLAIN, () -> this,
            requireNonNull(consumer, "StreamPlan consumer must be non-null"));
  }



  /**
   * A combination {@link Consumer} and {@link Supplier} used to provide the
   * {@link Spliterator} to a new {@code RecordStream}.
   *
   * @param <K> the key type of the {@code Record} instanced provided by the spliterator
   */
  private static final class SpliteratorSource<K extends Comparable<K>>
      implements Consumer<PipelineMetaData>, Supplier<Spliterator<Record<K>>> {

    private volatile Spliterator<Record<K>> spliterator;
    private RecordStreamImpl<K> recordStream;

    void setRecordStream(final RecordStreamImpl<K> recordStream) {
      assert recordStream != null;
      this.recordStream = recordStream;
    }

    /**
     * Traverses the pipeline meta data to determine the {@code Spliterator} instance to use.
     * This method is called by a terminal operation of a {@link WrappedReferenceStream}
     * implementation <i>before</i> the operation is invoked.
     *
     * @param metaData the pipeline meta data provided by the {@code WrappedReferenceStream}
     */
    @Override
    public void accept(final PipelineMetaData metaData) {
      requireNonNull(metaData, "metaData");
      if (this.recordStream == null) {
        throw new IllegalStateException("rcs not set");
      }
      this.spliterator = this.recordStream.parsePipeline(metaData);
    }

    /**
     * Gets the {@code Spliterator} created by the {@link #accept(PipelineMetaData) accept} method.
     * If the spliterator implements {@link AutoCloseable}, a close handler to close the spliterator is
     * added to the {@code Stream} provided to the
     * {@link #setRecordStream(RecordStreamImpl) setRecordStream} method.
     *
     * @return the {@code Spliterator} created from stream pipeline analysis
     */
    @Override
    public Spliterator<Record<K>> get() {
      final Spliterator<Record<K>> spliterator = this.spliterator;
      if (spliterator == null) {
        throw new IllegalStateException("spliterator not set by Consumer.accept");
      }
      if (spliterator instanceof AutoCloseable) {
        this.recordStream.associateCloseable((AutoCloseable)spliterator);
      }
      return spliterator;
    }
  }

  /**
   * Specifies the stream operations that may appear between the origin
   * {@code Stream} and a {@code filter} operation without affecting index
   * selection.
   * <p>
   * The operation pipeline begins with the first operation chained from this
   * {@code RecordStream}.  Some operations may be safely used between the
   * stream origin and the first {@code filter} operation without disturbing the
   * potential for using an index as the {@code Record} source.  {@code skip} and
   * {@code limit} are <b>excluded</b> from this list because, at the beginning of
   * the stream, the presumption is these operations imply working with all records.
   * {@code peek} is special -- it is safe if its {@code Consumer} is not a
   * {@code ManagedAction}.
   */
  private static final Set<? extends Operation> SAFE_BEFORE_FILTER = EnumSet.of(
      IntermediateOperation.DISTINCT,
      IntermediateOperation.ON_CLOSE,
      IntermediateOperation.PARALLEL,
      IntermediateOperation.SELF_CLOSE,
      IntermediateOperation.SEQUENTIAL,
      IntermediateOperation.SORTED_0,
      IntermediateOperation.SORTED_1,
      IntermediateOperation.UNORDERED,
      IntermediateOperation.EXPLAIN
  );

  /**
   * Specifies the stream operations that may appear between two {@code filter}
   * operations without affecting index selection.
   * <p>
   * Multiple {@code filter} operations may be chained in a sequence.  As at the
   * stream origin, some operations may come between {@code filter} operations
   * without affecting the potential for using an index.
   */
  private static final Set<? extends Operation> SAFE_BETWEEN_FILTERS = EnumSet.of(
      IntermediateOperation.DISTINCT,
      IntermediateOperation.ON_CLOSE,
      IntermediateOperation.PARALLEL,
      IntermediateOperation.SELF_CLOSE,
      IntermediateOperation.SEQUENTIAL,
      IntermediateOperation.SORTED_0,
      IntermediateOperation.SORTED_1,
      IntermediateOperation.UNORDERED,
      IntermediateOperation.EXPLAIN
  );

  /**
   * Specifies the stream operations that may appear between the origin
   * stream and an operation using a {@link ManagedAction}.
   * <p>
   * A {@code ManagedAction} instance consumes a {@code Record} so an operation
   * that uses a {@code ManagedAction} can only be preceded in the pipeline by
   * operations that do not change the stream element type from {@code Record}.
   * In addition to the following "safe" operations, {@code flatMap}, {@code map},
   * and {@code mapToObj} <i>may</i> emit {@code Record} but the actual type of
   * elements emitted by these streams is unknown without analysis.
   */
  private static final Set<? extends Operation> SAFE_BEFORE_MANAGED_ACTION = EnumSet.of(
      IntermediateOperation.DISTINCT,
      IntermediateOperation.FILTER,
      IntermediateOperation.LIMIT,
      IntermediateOperation.ON_CLOSE,
      IntermediateOperation.PARALLEL,
      IntermediateOperation.PEEK,     // Special handling in code
      IntermediateOperation.SELF_CLOSE,
      IntermediateOperation.SEQUENTIAL,
      IntermediateOperation.SKIP,
      IntermediateOperation.SORTED_0,
      IntermediateOperation.SORTED_1,
      IntermediateOperation.UNORDERED,
      IntermediateOperation.EXPLAIN
  );

  /**
   * Specifies the stream operations which may hold a {@link ManagedAction}
   * implementation.
   * <p>
   * A {@code ManagedAction} implementation (used for {@code Record} lock
   * management) can be used in any stream operation accepting a {@link Consumer}
   * or a {@link java.util.function.Function Function} that takes a {@code Record}
   * as input.
   *
   * @implNote Each of the operations listed takes a single argument; addition
   * of an operation that takes additional arguments needs to be coordinated
   * with the code in {@link #parsePipeline(PipelineMetaData)}.
   */
  private static final Set<? extends Operation> MAY_USE_MANAGED_ACTION =
      union(
          EnumSet.of(
              IntermediateOperation.FLAT_MAP,
              IntermediateOperation.FLAT_MAP_TO_DOUBLE,
              IntermediateOperation.FLAT_MAP_TO_INT,
              IntermediateOperation.FLAT_MAP_TO_LONG,
              IntermediateOperation.MAP,
              IntermediateOperation.MAP_TO_DOUBLE,
              IntermediateOperation.MAP_TO_INT,
              IntermediateOperation.MAP_TO_LONG
              // IntermediateOperation.MAP_TO_OBJ,    Omitted: primitive Stream method
              // IntermediateOperation.PEEK           *** EXPLICITLY DISABLED ***
          ),
          EnumSet.of(
              TerminalOperation.FOR_EACH,
              TerminalOperation.FOR_EACH_ORDERED
          )
      );

  /**
   * Analyzes stream pipeline meta data and instantiates the {@link Spliterator} used to
   * feed this stream.  This method is called using the <i>terminal action</i>
   * {@link RecordStreamImpl.SpliteratorSource SpliteratorSource}
   * set by {@link #newInstance(Catalog, boolean)}.
   *
   * @param metaData the {@code PipelineMetaData} instance describing the stream pipeline
   *
   * @return a new {@code Spliterator} to provide {@code Record}s to this stream
   */
  private Spliterator<Record<K>> parsePipeline(final PipelineMetaData metaData) {
    assert metaData != null;

    StreamPlanWriter streamPlanWriter = createQueryPlanWriter(metaData.getPipeline());

    ManagedAction<K> managedAction = null;
    streamPlanWriter.startPlanning();

    /* Loop through the pipeline of operations to:
     *   1) determine what filter Predicates should influence index selection;
     *   2) identify the ManagedAction, if any;
     *   3) identify the ManagedAction insertion point, if any.
     *
     * Unless the filter predicate is an comparison object,
     * the optimizer layer that "inspects" the predicate cannot influence index
     * selection. Use the reference {@code startComparison} to track this.
     *
     * Lock management provided by the ManagedAction, if any, must follow the last
     * operation capable of affecting stream element delivery order (distinct, sorted,
     * skip, limit).  During pipeline construction, each such operation is prepared with
     * a ManagedAction "insertion point".  See TDB-1350 for details.
     */
    IntrinsicPredicate<Record<K>> startComparison = null;
    ManagedActionSupplier<K> managedActionInsertionPoint = null;
    boolean collectingFilterPredicates = true;
    boolean seekingManagedAction = true;
    boolean beforeFilter = true;
    for (final PipelineOperation pipelineOperation : metaData.getPipeline()) {
      final Operation operation = pipelineOperation.getOperation();
      final List<Object> arguments = pipelineOperation.getArguments();
      final OperationMetaData operationMetaData = pipelineOperation.getOperationMetaData();

      /*
       * Collect filter predicates from origin stream until an "unsafe" operation is encountered.
       */
      if (operation == IntermediateOperation.FILTER) {
        beforeFilter = false;
        final Object filterArgument = arguments.get(0);
        if (filterArgument instanceof Predicate) {
          if (filterArgument instanceof IntrinsicPredicate<?>) {
            // we can do optimization if and only if there is at least one describable predicate
            @SuppressWarnings("unchecked")
            final IntrinsicPredicate<Record<K>> filterPredicate = (IntrinsicPredicate<Record<K>>)filterArgument;
            if (collectingFilterPredicates) {
              startComparison = (startComparison == null) ? filterPredicate
                  : (IntrinsicPredicate<Record<K>>)startComparison.and(filterPredicate);
            } else {
              streamPlanWriter.addUnusedFilterExpression(filterPredicate.toString());
            }
          } else {
            // stop collecting to avoid applying unsafe checks.
            collectingFilterPredicates = false;
            streamPlanWriter.incrementUnknownFilter();
          }
        } else {
          throw new IllegalStateException("filter argument not Predicate: " + filterArgument.getClass().getName());
        }
      } else {
        collectingFilterPredicates = (beforeFilter ? SAFE_BEFORE_FILTER : SAFE_BETWEEN_FILTERS).contains(operation);
      }

      /* Identify the last ManagedAction insertion point, if any, while capturing the
       * first, and hopefully only, ManagedAction used in an operation.
       * PEEK is special -- technically it _could_ hold a ManagedAction but, given that PEEK
       * is supposed to be without side effect, is should not and will be disallowed here.
       */
      if (seekingManagedAction) {
        if (operationMetaData instanceof ManagedActionSupplier) {
          @SuppressWarnings("unchecked")
          final ManagedActionSupplier<K> managedActionSupplier = (ManagedActionSupplier<K>)operationMetaData;
          managedActionInsertionPoint = managedActionSupplier;
        }
        if (operation == IntermediateOperation.PEEK) {
          /* A PEEK operation does not affect the Stream type and, as long as it
           * does *not* hold a ManagedAction, it does not affect the search for
           * a ManagedAction.
           */
          if (arguments.get(0) instanceof ManagedAction) {
            throw new IllegalArgumentException("peek argument may not be a ManagedAction");
          }
        } else if (MAY_USE_MANAGED_ACTION.contains(operation)) {
          /* The operations that may hold a ManagedAction are either terminal or
           * map to a Stream of a different element type.  Reaching one of these
           * operations ends the search for a ManagedAction regardless of whether or
           * not the operation contains one.
           */
          // TODO: Find a way to determine if flatMap, map, and mapToObj return Stream<Record<K>>
          // If flatMap, map, or mapToObj emit a Stream<Record<K>> and don't hold a ManagedAction,
          // those operations might be safely "passed" here without ceasing the search for
          // ManagedAction.
          seekingManagedAction = false;
          if (arguments.size() != 1) {
            throw new IllegalStateException("unexpected " + operation.name() + " arguments: count=" + arguments.size());
          }
          final Object argument = arguments.get(0);
          if (argument instanceof ManagedAction) {
            @SuppressWarnings("unchecked")
            final ManagedAction<K> operationArgument = (ManagedAction<K>)argument;    // unchecked
            managedAction = operationArgument;
          }
        } else {
          seekingManagedAction = SAFE_BEFORE_MANAGED_ACTION.contains(operation);
        }
      }
    }

    /*
     * ManagedAction lock management must follow all stateful intermediate operations capable of
     * affecting element delivery order instead of being performed by the stream source
     * Spliterator.  Provide the ManagedAction to the insertion point related to the last such
     * operation and remove the ManagedAction from consideration of the source Spliterator.
     */
    if (managedActionInsertionPoint != null && managedAction != null) {
      managedActionInsertionPoint.setManagedAction(managedAction);
      managedAction = null;
    }

    /*
     * At this point, the stream pipeline has been examined for 'filter' operations and the use
     * of ManagedAction.  Time to perform index selection analysis ...
     */
    Optimization<K> result = getOptimization(startComparison, streamPlanWriter);

    @SuppressWarnings("unchecked")    // Eclipse accommodation  https://bugs.eclipse.org/bugs/show_bug.cgi?id=470849
    Spliterator<Record<K>> spliterator = getSpliterator(this, this.generator, result, managedAction);
    streamPlanWriter.endPlanning();

    return spliterator;
  }

  private Optimization<K> getOptimization(IntrinsicPredicate<Record<K>> startComparison, StreamPlanWriter streamPlanWriter) {
    if (startComparison == null) {
      return Optimization.emptyOptimisation();
    } else {
      // an attempt to optimize can be made as we have comparison predicates in the stream filters...
      streamPlanWriter.setUsedFilterExpression(startComparison.toString());
      Optimization<K> optimization = SovereignOptimizerFactory.newInstance(generator).optimize(startComparison);
      streamPlanWriter.setResult(optimization);
      return optimization;
    }
  }

  private static StreamPlanWriter createQueryPlanWriter(List<PipelineOperation> pipeline) {
    Collection<Consumer<StreamPlan>> planConsumers = new ArrayList<>();
    for (final PipelineOperation pipelineOperation : pipeline) {
      if (pipelineOperation.getOperation().equals(IntermediateOperation.EXPLAIN)) {
        @SuppressWarnings("unchecked")
        Consumer<StreamPlan> consumer = (Consumer<StreamPlan>) pipelineOperation.getArguments().get(0);
        planConsumers.add(consumer);
      }
    }
    if (planConsumers.isEmpty()) {
      return StreamPlanTracker.getDummyWriter();
    } else {
      return new StreamPlanTracker(planConsumers);
    }
  }

  /**
   * Generate a new {@link Spliterator} over an {@link RecordStreamImpl} using the
   * {@link LocatorGenerator} provided optimized for the {@link CellComparison} and
   * {@link ManagedAction} provided, if any.
   *
   * @param recordStream the {@code RecordStream} on which the new {@code Spliterator} is based
   * @param generator the {@code LocatorGenerator} from which the {@link Spliterator} is obtained
   * @param optimizationResult the {@code Optimization}, if any, for which the new {@code Spliterator} should be optimized
   * @param managedAction the first {@code ManagedAction}, if any, through which records are accessed
   * @param <K> the type of the {@code Record} key
   *
   * @return a new {@code Spliterator}
   */
  private static <K extends Comparable<K>> Spliterator<Record<K>> getSpliterator(
      final RecordStreamImpl<K> recordStream,
      final LocatorGenerator<K> generator,
      final Optimization<K> optimizationResult,
      final ManagedAction<K> managedAction) {
    assert recordStream != null;
    assert generator != null;

    if (optimizationResult.doSkipScan()) {
      return new SkippingSpliterator<>();
    } else if (optimizationResult.getCellComparison() == null && managedAction == null) {
      // Use primary index -- all records are presented
      return generator.createSpliterator(recordStream);
    } else {
      // Let LocatorGenerator determine index use ... may be primary; may be secondary
      return generator.createSpliterator(recordStream, optimizationResult.getCellComparison(), managedAction);
    }
  }

  /**
   * Functionally joins two {@link Set}s.
   *
   * @param first the first {@code Set}
   * @param second the second {@code Set}
   * @param <T> the base element type of both {@code first} and {@code second}
   *
   * @return an unmodifiable {@code Set} delegating operations to {@code first} then {@code second}
   */
  private static <T> Set<? extends T> union(final Set<? extends T> first, final Set<? extends T> second) {
    return new AbstractSet<T>() {
      @Override
      public boolean contains(final Object o) {
        return first.contains(o) || second.contains(o);
      }

      @SuppressWarnings("NullableProblems")
      @Override
      public Iterator<T> iterator() {
        return Stream.concat(first.stream(), second.stream()).iterator();
      }

      @Override
      public int size() {
        return first.size() + second.size();
      }
    };
  }

  /**
   * An {@code OperationMetaData} implementation used to supply the {@code ManagedAction}, if any,
   * to the point following a stateful intermediate operation like {@code sorted}.
   *
   * @param <K> the {@code ManagedAction} key type
   */
  private static final class ManagedActionSupplier<K extends Comparable<K>>
      implements Supplier<ManagedAction<K>>, OperationMetaData {

    /**
     * The {@code ManagedAction} to supply.  If not set using
     * {@link #setManagedAction(ManagedAction)}, a {@code ManagedAction}
     * instance relying on the {@code default} implementations of
     * {@link ManagedAction#begin(InternalRecord) begin} and
     * {@link ManagedAction#end(InternalRecord) end} is used.
     */
    private volatile ManagedAction<K> managedAction = () -> null;

    public void setManagedAction(final ManagedAction<K> managedAction) {
      requireNonNull(managedAction, "managedAction");
      this.managedAction = managedAction;
    }

    @Override
    public ManagedAction<K> get() {
      return this.managedAction;
    }
  }

  private static class SkippingSpliterator<K extends Comparable<K>> implements Spliterator<Record<K>> {
    @Override
    public boolean tryAdvance(Consumer<? super Record<K>> action) {
      return false;
    }

    @Override
    public Spliterator<Record<K>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public int characteristics() {
      return 0;
    }
  }
}
