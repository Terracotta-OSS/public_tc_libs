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
package com.terracottatech.store.server.stream;

import com.terracottatech.sovereign.RecordStream;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorRequestResultMessage;
import com.terracottatech.store.common.messages.stream.PipelineRequestProcessingResponse;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineMessage;
import com.terracottatech.store.common.messages.stream.terminated.ExecuteTerminatedPipelineResponse;
import org.junit.Test;
import org.mockito.Answers;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.platform.ServerInfo;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.DELETE_THEN;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.COUNT;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.DELETE;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TerminatedPipelineProcessorTest {

  @Test
  public void testClosesDatasetStream() {
    RecordStream<String> stream = mock(RecordStream.class);
    SovereignDataset<String> dataset = mock(SovereignDataset.class);
    when(dataset.records()).thenReturn(stream);
    when(dataset.getAlias()).thenReturn("testDataset");
    TerminatedPipelineProcessor<String> processor = new TerminatedPipelineProcessor<>(mock(Executor.class), mock(IEntityMessenger.class), mock(ServerInfo.class), mock(ClientDescriptor.class),
            UUID.randomUUID(), dataset, Collections.emptyList(), COUNT.newInstance(), false);

    processor.close();

    verify(stream).close();
  }

  /**
   * Stream closure doesn't currently cause anything to perturb the streams execution.  If that changes this test will
   * catch that.
   */
  @Test
  public void testCloseStreamDuringStreamExecution() throws BrokenBarrierException, InterruptedException {
    CyclicBarrier barrier = new CyclicBarrier(2);

    SovereignDataset<Integer> dataset = new SovereignBuilder<>(Type.INT, SystemTimeReference.class).build();
    range(0, 100).forEach(k -> dataset.add(SovereignDataset.Durability.IMMEDIATE, k));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      TerminatedPipelineProcessor<Integer> processor = new TerminatedPipelineProcessor<>(executor, mock(IEntityMessenger.class), mock(ServerInfo.class), mock(ClientDescriptor.class),
              UUID.randomUUID(), dataset, singletonList(FILTER.newInstance((Predicate<Record<Integer>>) r -> {
        if (r.getKey() == 50) {
          try {
            barrier.await();
            barrier.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
          }
          return false;
        } else {
          return true;
        }
      })), DELETE.newInstance(), false);

      processor.invoke(executeMessage());

      barrier.await();
      processor.close();
      barrier.await();

      try {
        processor.invoke(requestResultMessage());
        fail("Expected StoreRuntimeException");
      } catch (StoreRuntimeException e) {
        assertThat(e.getMessage(), containsString("pipeline processor closed"));
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testCloseStreamDuringAsyncStreamExecution() throws BrokenBarrierException, InterruptedException {
    CyclicBarrier barrier = new CyclicBarrier(2);

    SovereignDataset<Integer> dataset = new SovereignBuilder<>(Type.INT, SystemTimeReference.class).build();
    range(0, 100).forEach(k -> dataset.add(SovereignDataset.Durability.IMMEDIATE, k));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      TerminatedPipelineProcessor<Integer> processor = new TerminatedPipelineProcessor<>(executor, mock(IEntityMessenger.class), mock(ServerInfo.class), mock(ClientDescriptor.class),
              UUID.randomUUID(), dataset, singletonList(FILTER.newInstance((Predicate<Record<Integer>>) r -> {
        if (r.getKey() == 50) {
          try {
            barrier.await();
            barrier.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
          }
          return false;
        } else {
          return true;
        }
      })), DELETE.newInstance(), false);

      processor.invoke(executeMessage());

      barrier.await();
      processor.close();
      barrier.await();

      try {
        processor.invoke(requestResultMessage());
        fail("Expected StoreRuntimeException");
      } catch (StoreRuntimeException e) {
        //expected
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testCloseDatasetDuringStreamExecution() throws Exception {
    CyclicBarrier barrier = new CyclicBarrier(2);

    SovereignDataset<Integer> dataset = new SovereignBuilder<>(Type.INT, SystemTimeReference.class).build();
    range(0, 100).forEach(k -> dataset.add(SovereignDataset.Durability.IMMEDIATE, k));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      TerminatedPipelineProcessor<Integer> processor = new TerminatedPipelineProcessor<>(executor, mock(IEntityMessenger.class), mock(ServerInfo.class), mock(ClientDescriptor.class),
              UUID.randomUUID(), dataset, singletonList(FILTER.newInstance((Predicate<Record<Integer>>) r -> {
        if (r.getKey() == 50) {
          try {
            barrier.await();
            barrier.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
          }
          return false;
        } else {
          return true;
        }
      })), DELETE.newInstance(), false);

      processor.invoke(executeMessage());

      barrier.await();
      dataset.getStorage().shutdown();
      barrier.await();

      try {
        processor.invoke(requestResultMessage());
        fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        //expected
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testRequestResultReturnsStreamResult() {
    RecordStream<String> stream = mock(RecordStream.class);
    when(stream.map(any())).then(Answers.RETURNS_SELF);
    when(stream.count()).thenReturn(42L);

    SovereignDataset<String> dataset = mock(SovereignDataset.class);
    when(dataset.records()).thenReturn(stream);
    when(dataset.getAlias()).thenReturn("testDataset");

    TerminatedPipelineProcessor<String> processor = new TerminatedPipelineProcessor<>(ForkJoinPool.commonPool(), mock(IEntityMessenger.class), mock(ServerInfo.class), mock(ClientDescriptor.class),
            UUID.randomUUID(), dataset, singletonList(DELETE_THEN.newInstance()), COUNT.newInstance(), false);

    processor.invoke(executeMessage());

    assertThat(((ExecuteTerminatedPipelineResponse) processor.invoke(requestResultMessage())).getElementValue().getValue(), is(42L));
  }

  @Test
  public void testGetResultPropagatesException() {
    RecordStream<String> stream = mock(RecordStream.class);
    when(stream.map(any())).then(Answers.RETURNS_SELF);
    when(stream.count()).thenThrow(IllegalStateException.class);

    SovereignDataset<String> dataset = mock(SovereignDataset.class);
    when(dataset.records()).thenReturn(stream);
    when(dataset.getAlias()).thenReturn("testDataset");

    TerminatedPipelineProcessor<String> processor = new TerminatedPipelineProcessor<>(ForkJoinPool.commonPool(), mock(IEntityMessenger.class), mock(ServerInfo.class), mock(ClientDescriptor.class),
            UUID.randomUUID(), dataset, singletonList(DELETE_THEN.newInstance()), COUNT.newInstance(), false);

    processor.invoke(executeMessage());

    try {
      processor.invoke(requestResultMessage());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }
  }

  @Test
  public void testGetResultPreservesInterrupt() {
    RecordStream<String> stream = mock(RecordStream.class);
    when(stream.map(any())).then(Answers.RETURNS_SELF);
    when(stream.count()).thenReturn(42L);

    SovereignDataset<String> dataset = mock(SovereignDataset.class);
    when(dataset.records()).thenReturn(stream);
    when(dataset.getAlias()).thenReturn("testDataset");

    TerminatedPipelineProcessor<String> processor = new TerminatedPipelineProcessor<>(ForkJoinPool.commonPool(), mock(IEntityMessenger.class), mock(ServerInfo.class), mock(ClientDescriptor.class),
            UUID.randomUUID(), dataset, singletonList(DELETE_THEN.newInstance()), COUNT.newInstance(), false);

    processor.invoke(executeMessage());

    Thread.currentThread().interrupt();
    assertThat(((ExecuteTerminatedPipelineResponse) processor.invoke(requestResultMessage())).getElementValue().getValue(), is(42L));
    assertThat(Thread.interrupted(), is(true));
  }

  @Test
  public void testResultCalculationTriggersComplete() throws InterruptedException {
    RecordStream<String> stream = mock(RecordStream.class);
    when(stream.count()).thenReturn(42L);

    SovereignDataset<String> dataset = mock(SovereignDataset.class);
    when(dataset.records()).thenReturn(stream);
    when(dataset.getAlias()).thenReturn("testDataset");

    IEntityMessenger<EntityMessage, ?> messenger = mock(IEntityMessenger.class);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    TerminatedPipelineProcessor<String> processor = new TerminatedPipelineProcessor<>(executor, messenger, mock(ServerInfo.class), mock(ClientDescriptor.class),
        UUID.randomUUID(), dataset, Collections.emptyList(), DELETE.newInstance(), false);
    try {
      assertThat(processor.invoke(executeMessage()), instanceOf(PipelineRequestProcessingResponse.class));
    } finally {
      executor.shutdown();
    }
    executor.awaitTermination(30, TimeUnit.SECONDS);
    assertThat(processor.isComplete(), is(true));

    processor.invoke(requestResultMessage());

    verify(stream).close();
  }

  private static PipelineProcessorRequestResultMessage requestResultMessage() {
    PipelineProcessorRequestResultMessage message = mock(PipelineProcessorRequestResultMessage.class);
    when(message.getType()).thenReturn(DatasetOperationMessageType.PIPELINE_REQUEST_RESULT_MESSAGE);
    return message;
  }

  private static ExecuteTerminatedPipelineMessage executeMessage() {
    ExecuteTerminatedPipelineMessage message = mock(ExecuteTerminatedPipelineMessage.class);
    when(message.getType()).thenReturn(DatasetOperationMessageType.EXECUTE_TERMINATED_PIPELINE_MESSAGE);
    return message;
  }
}
