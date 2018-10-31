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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.Record;
import com.terracottatech.store.client.AnimalsDataset;
import com.terracottatech.store.client.VoltronDatasetEntity;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.common.messages.stream.ElementValue;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.test.data.Animals.Animal;
import org.junit.rules.ExternalResource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Spliterator;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * An {@link ExternalResource} implementation that provides a mocked {@link RootRemoteRecordStream}
 * populated with {@link com.terracottatech.test.data.Animals#ANIMALS ANIMALS}.
 */
public final class EmbeddedAnimalDataset extends ExternalResource {

  @Mock
  private VoltronDatasetEntity<String> datasetEntity;

  private AnimalsDataset.Container datasetContainer;
  private Dataset<String> embeddedDataset;

  @Override
  protected void before() throws Throwable {
    super.before();

    MockitoAnnotations.initMocks(this);
    this.datasetContainer = AnimalsDataset.createDataset(16L, MemoryUnit.MB);
    this.embeddedDataset = this.datasetContainer.getDataset();
    when(datasetEntity.nonMutableStream()).thenAnswer(invocationOnMock -> new RootRemoteRecordStream<>(datasetEntity));
    when(datasetEntity.spliterator(any(RootStreamDescriptor.class)))
        .thenAnswer(invocationOnMock -> {
              Object[] arguments = invocationOnMock.getArguments();
              @SuppressWarnings("unchecked")
              List<PipelineOperation> portableOperations = ((RootStreamDescriptor)arguments[0]).getPortableIntermediatePipelineSequence();
              return getSpliterator(portableOperations);
            }
        );
    when(datasetEntity.doubleSpliterator(any(RootStreamDescriptor.class)))
        .thenAnswer(invocationOnMock -> {
              Object[] arguments = invocationOnMock.getArguments();
              @SuppressWarnings("unchecked")
              List<PipelineOperation> portableOperations = ((RootStreamDescriptor)arguments[0]).getPortableIntermediatePipelineSequence();
              return doubleSpliterator(portableOperations);
            }
        );
    when(datasetEntity.intSpliterator(any(RootStreamDescriptor.class)))
        .thenAnswer(invocationOnMock -> {
              Object[] arguments = invocationOnMock.getArguments();
              @SuppressWarnings("unchecked")
              List<PipelineOperation> portableOperations = ((RootStreamDescriptor)arguments[0]).getPortableIntermediatePipelineSequence();
              return intSpliterator(portableOperations);
            }
        );
    when(datasetEntity.longSpliterator(any(RootStreamDescriptor.class)))
        .thenAnswer(invocationOnMock -> {
              Object[] arguments = invocationOnMock.getArguments();
              @SuppressWarnings("unchecked")
              List<PipelineOperation> portableOperations = ((RootStreamDescriptor)arguments[0]).getPortableIntermediatePipelineSequence();
              return longSpliterator(portableOperations);
            }
        );
    when(datasetEntity.objSpliterator(any(RootStreamDescriptor.class)))
        .thenAnswer(invocationOnMock -> {
              Object[] arguments = invocationOnMock.getArguments();
              @SuppressWarnings("unchecked")
              List<PipelineOperation> portableOperations = ((RootStreamDescriptor)arguments[0]).getPortableIntermediatePipelineSequence();
              return objSpliterator(portableOperations);
            }
        );
    when(datasetEntity.executeTerminatedStream(any(RootStreamDescriptor.class)))
        .thenAnswer(invocationOnMock -> {
              Object[] arguments = invocationOnMock.getArguments();
              RootStreamDescriptor rootStreamDescriptor = (RootStreamDescriptor)arguments[0];
              List<PipelineOperation> portableOperations = rootStreamDescriptor.getPortableIntermediatePipelineSequence();
              PipelineOperation terminalOperation = rootStreamDescriptor.getPortableTerminalOperation();
              Object o = executeTerminatedStream(portableOperations, terminalOperation);
              return ElementValue.createForObject(o);
            }
        );
  }

  @Override
  protected void after() {
    if (this.datasetContainer != null) {
      this.datasetContainer.close();
    }
    super.after();
  }


  /**
   * Gets a new {@link RootRemoteRecordStream} over a dataset populated with {@link Animal Animal} records.
   * @return a new animal stream
   */
  public RootRemoteRecordStream<String> getStream() {
    return (RootRemoteRecordStream<String>) datasetEntity.nonMutableStream();
  }

  private Spliterator<Record<String>> getSpliterator(List<PipelineOperation> portableOperations) {
    Stream<Record<String>> recordSource =
        this.embeddedDataset.reader().records();
    return new AutoClosingSpliterator<>(
        new TestRemoteSpliterator<>(this.<Record<String>, Stream<Record<String>>>reconstructPipeline(recordSource, portableOperations).spliterator()),
        recordSource);
  }

  private <T> Spliterator<T> objSpliterator(List<PipelineOperation> portableOperations) {
    Stream<Record<String>> recordSource =
        this.embeddedDataset.reader().records();
    return new AutoClosingSpliterator<>(
        new TestRemoteSpliterator<>(this.<T, Stream<T>>reconstructPipeline(recordSource, portableOperations).spliterator()),
        recordSource);
  }

  private Object executeTerminatedStream(List<PipelineOperation> portableOperations, PipelineOperation terminalOperation) {
    Stream<Record<String>> recordSource =
        this.embeddedDataset.reader().records();
    return executePipeline(recordSource, portableOperations, terminalOperation);
  }

  private Spliterator.OfDouble doubleSpliterator(List<PipelineOperation> portableOperations) {
    Stream<Record<String>> recordSource =
        this.embeddedDataset.reader().records();
    return new AutoClosingSpliteratorOfDouble<>(
        new TestRemoteSpliteratorOfDouble(this.<Double, DoubleStream>reconstructPipeline(recordSource, portableOperations).spliterator()),
        recordSource);
  }

  private Spliterator.OfInt intSpliterator(List<PipelineOperation> portableOperations) {
    Stream<Record<String>> recordSource =
        this.embeddedDataset.reader().records();
    return new AutoClosingSpliteratorOfInt<>(
        new TestRemoteSpliteratorOfInt(this.<Integer, IntStream>reconstructPipeline(recordSource, portableOperations).spliterator()),
        recordSource);
  }

  private Spliterator.OfLong longSpliterator(List<PipelineOperation> portableOperations) {
    Stream<Record<String>> recordSource =
        this.embeddedDataset.reader().records();
    return new AutoClosingSpliteratorOfLong<>(
        new TestRemoteSpliteratorOfLong(this.<Long, LongStream>reconstructPipeline(recordSource, portableOperations).spliterator()),
        recordSource);
  }

  @SuppressWarnings("unchecked")
  private <T, S extends BaseStream<T, S>>
  S reconstructPipeline(Stream<Record<String>> rootStream, List<PipelineOperation> portableOperations) {
    BaseStream<?, ?> stream = rootStream;

    for (PipelineOperation pipelineOperation : portableOperations) {
      PipelineOperation.Operation op = pipelineOperation.getOperation();
      stream = (BaseStream)op.reconstruct(stream, pipelineOperation.getArguments());    // unchecked
    }

    return (S)stream;     // unchecked
  }

  private Object executePipeline(Stream<Record<String>> rootStream, List<PipelineOperation> portableOperations, PipelineOperation terminalOperation) {
    BaseStream<?, ?> stream = rootStream;

    for (PipelineOperation pipelineOperation : portableOperations) {
      PipelineOperation.Operation op = pipelineOperation.getOperation();
      stream = (BaseStream)op.reconstruct(stream, pipelineOperation.getArguments());    // unchecked
    }

    return terminalOperation.getOperation().reconstruct(stream, terminalOperation.getArguments());
  }


  private static final class AutoClosingSpliterator<T>
      extends AbstractAutoClosingSpliterator<T, RemoteSpliterator<T>, Spliterator<T>> {

    AutoClosingSpliterator(RemoteSpliterator<T> delegate, BaseStream<?, ?> headStream) {
      super(delegate, headStream);
    }

    private AutoClosingSpliterator(AutoClosingSpliterator<T> delegate, Spliterator<T> split) {
      super(delegate, split);
    }

    @Override
    protected Spliterator<T> chainedSpliterator(Spliterator<T> split) {
      return new AutoClosingSpliterator<>(this, split);
    }
  }

  static final class AutoClosingSpliteratorOfDouble<R_SPLIT extends Spliterator.OfDouble & RemoteSpliterator<Double>>
      extends PrimitiveAutoClosingSpliterator<Double, DoubleConsumer, Spliterator.OfDouble, R_SPLIT>
      implements RemoteSpliterator.OfDouble {

    AutoClosingSpliteratorOfDouble(R_SPLIT delegate, BaseStream<?, ?> headStream) {
      super(delegate, headStream);
    }

    private AutoClosingSpliteratorOfDouble(AutoClosingSpliteratorOfDouble<R_SPLIT> owner, Spliterator.OfDouble split) {
      super(owner, split);
    }

    @Override
    protected Spliterator.OfDouble chainedSpliterator(Spliterator.OfDouble split) {
      return new AutoClosingSpliteratorOfDouble<>(this, split);
    }
  }

  static final class AutoClosingSpliteratorOfInt<R_SPLIT extends Spliterator.OfInt & RemoteSpliterator<Integer>>
      extends PrimitiveAutoClosingSpliterator<Integer, IntConsumer, Spliterator.OfInt, R_SPLIT>
      implements RemoteSpliterator.OfInt {

    AutoClosingSpliteratorOfInt(R_SPLIT delegate, BaseStream<?, ?> headStream) {
      super(delegate, headStream);
    }

    private AutoClosingSpliteratorOfInt(AutoClosingSpliteratorOfInt<R_SPLIT> owner, Spliterator.OfInt split) {
      super(owner, split);
    }

    @Override
    protected Spliterator.OfInt chainedSpliterator(Spliterator.OfInt split) {
      return new AutoClosingSpliteratorOfInt<>(this, split);
    }
  }

  static final class AutoClosingSpliteratorOfLong<R_SPLIT extends Spliterator.OfLong & RemoteSpliterator<Long>>
      extends PrimitiveAutoClosingSpliterator<Long, LongConsumer, Spliterator.OfLong, R_SPLIT>
      implements RemoteSpliterator.OfLong {

    AutoClosingSpliteratorOfLong(R_SPLIT delegate, BaseStream<?, ?> headStream) {
      super(delegate, headStream);
    }

    private AutoClosingSpliteratorOfLong(AutoClosingSpliteratorOfLong<R_SPLIT> owner, Spliterator.OfLong split) {
      super(owner, split);
    }

    @Override
    protected Spliterator.OfLong chainedSpliterator(Spliterator.OfLong split) {
      return new AutoClosingSpliteratorOfLong<>(this, split);
    }
  }
}
