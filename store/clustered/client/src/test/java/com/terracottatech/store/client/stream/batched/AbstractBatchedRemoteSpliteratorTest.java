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
package com.terracottatech.store.client.stream.batched;

import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.client.stream.RootStreamDescriptor;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.stream.Element;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchMessage;
import com.terracottatech.store.common.messages.stream.batched.BatchFetchResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.UUID;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractBatchedRemoteSpliteratorTest {

  @Test
  public void testTryAdvanceWithNullConsumer() {
    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(mock(MessageSender.class));
    try {
      spliterator.tryAdvance(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public void testTryAdvanceWhenClosedThrows() {
    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(mock(MessageSender.class));
    spliterator.close();
    try {
      spliterator.tryAdvance(l -> fail());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }
  }

  @Test
  public void testTryAdvanceWhenConsumedReturnsFalseEvenIfClosed() {
    MessageSender messageSender = mock(MessageSender.class);
    when(messageSender.sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class))).thenReturn(
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf())));

    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(messageSender);

    spliterator.forEachRemaining(s -> {});
    spliterator.close();
    assertThat(spliterator.tryAdvance(l -> fail()), is(false));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLeadingTryAdvanceFetchesBatch() {
    MessageSender messageSender = mock(MessageSender.class);
    when(messageSender.sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class))).thenReturn(
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf(1L, 2L, 3L))),
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf())));
    clearInvocations(messageSender);

    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(messageSender);

    List<Long> results = new ArrayList<>();
    assertThat(spliterator.tryAdvance(results::add), is(true));
    verify(messageSender, times(2)).sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class));

    assertThat(spliterator.tryAdvance(results::add), is(true));
    assertThat(spliterator.tryAdvance(results::add), is(true));
    assertThat(spliterator.tryAdvance(results::add), is(false));
    verify(messageSender, times(2)).sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class));

    assertThat(results, contains(1L, 2L, 3L));
  }

  @Test
  public void testForEachRemainingWithNullConsumer() {
    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(mock(MessageSender.class));
    try {
      spliterator.forEachRemaining(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  @Test
  public void testForEachRemainingWhenClosedThrows() {
    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(mock(MessageSender.class));
    spliterator.close();
    try {
      spliterator.forEachRemaining(l -> fail());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      //expected
    }
  }

  @Test
  public void testForEachRemainingWhenConsumedReturnsCleanly() {
    MessageSender messageSender = mock(MessageSender.class);
    when(messageSender.sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class))).thenReturn(
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf())));

    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(messageSender);

    spliterator.forEachRemaining(s -> {});
    spliterator.close();
    spliterator.forEachRemaining(l -> fail());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLeadingForEachRemainingFetchesBatch() {
    MessageSender messageSender = mock(MessageSender.class);
    when(messageSender.sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class))).thenReturn(
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf(1L, 2L, 3L))),
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf())));
    clearInvocations(messageSender);

    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(messageSender);

    List<Long> results = new ArrayList<>();
    spliterator.forEachRemaining(results::add);
    verify(messageSender, times(2)).sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class));
    assertThat(results, contains(1L, 2L, 3L));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testLeadingTrySplitTriggersBatchFetch() {
    MessageSender messageSender = mock(MessageSender.class);
    when(messageSender.sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class))).thenReturn(
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf(1L, 2L, 3L))),
        completedFuture(new BatchFetchResponse(UUID.randomUUID(), batchOf())));
    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(messageSender);
    assertThat(spliterator.trySplit(), nullValue());

    Spliterator<Long> batch = spliterator.trySplit();
    List<Long> results = new ArrayList<>();
    batch.forEachRemaining(results::add);
    verify(messageSender, times(2)).sendMessage(any(BatchFetchMessage.class), any(SendConfiguration.class));
    assertThat(results, contains(1L, 2L, 3L));

    spliterator.forEachRemaining(l -> fail());
  }

  @Test
  public void testSuppressReleaseIsIllegal() {
    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(mock(MessageSender.class));

    try {
      spliterator.suppressRelease();
      fail("Expected AssertionError");
    } catch (AssertionError e) {
      //expected
    }
  }

  @Test
  public void testSendReceiveDelegatesCorrectly() {
    PipelineProcessorMessage message = mock(PipelineProcessorMessage.class);
    DatasetEntityResponse response = mock(DatasetEntityResponse.class);

    MessageSender messageSender = mock(MessageSender.class);
    when(messageSender.sendMessage(message, SendConfiguration.ONE_SERVER)).thenReturn(completedFuture(response));
    AbstractBatchedRemoteSpliterator<Long> spliterator = createSpliterator(messageSender);

    assertThat(spliterator.sendReceive("testing", message), sameInstance(response));
  }

  private static AbstractBatchedRemoteSpliterator<Long> createSpliterator(MessageSender messageSender) {
    return new BatchedRemoteSpliteratorOfLong(
        messageSender,
        UUID.randomUUID(),
        mock(RootStreamDescriptor.class),
        () -> false) {};
  }

  private static List<Element> batchOf(Long... values) {
    return Arrays.stream(values).map(s -> new Element(ElementType.LONG, s)).collect(toList());
  }
}