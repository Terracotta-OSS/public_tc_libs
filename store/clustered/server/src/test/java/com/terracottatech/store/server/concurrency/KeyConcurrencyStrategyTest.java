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
package com.terracottatech.store.server.concurrency;

import com.terracottatech.store.common.messages.crud.AddRecordMessage;
import com.terracottatech.store.common.messages.crud.GetRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedDeleteRecordMessage;
import com.terracottatech.store.common.messages.crud.PredicatedUpdateRecordMessage;
import com.terracottatech.store.common.messages.event.SendChangeEventsMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorCloseMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorRequestResultMessage;
import com.terracottatech.store.common.messages.stream.RemoteStreamType;
import com.terracottatech.store.common.messages.stream.PipelineProcessorOpenMessage;
import com.terracottatech.store.common.messages.stream.ElementType;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceFetchMessage;
import com.terracottatech.store.common.messages.stream.inline.TryAdvanceReleaseMessage;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import org.junit.Test;
import org.terracotta.entity.ConcurrencyStrategy;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class KeyConcurrencyStrategyTest {
  private final UUID stableClientId = UUID.randomUUID();

  @Test
  public void universalKeyForGets() {
    KeyConcurrencyStrategy<Integer> strategy = new KeyConcurrencyStrategy<>(new KeyHashCodeConcurrencyStrategy<Integer>(), mock(ReplicationConcurrencyStrategy.class));
    assertEquals(ConcurrencyStrategy.UNIVERSAL_KEY, strategy.concurrencyKey(new GetRecordMessage<>("key", AlwaysTrue.alwaysTrue())));
  }

  @Test
  public void managementKeyForSendChangeEvents() {
    KeyConcurrencyStrategy<Integer> strategy = new KeyConcurrencyStrategy<>(new KeyHashCodeConcurrencyStrategy<Integer>(), mock(ReplicationConcurrencyStrategy.class));
    assertEquals(ConcurrencyStrategy.MANAGEMENT_KEY, strategy.concurrencyKey(new SendChangeEventsMessage(true)));
  }

  @Test
  public void validConcurrencyKeyForDeleteMessages() {
    KeyConcurrencyStrategy<Integer> strategy = new KeyConcurrencyStrategy<>(new KeyHashCodeConcurrencyStrategy<Integer>(), mock(ReplicationConcurrencyStrategy.class));
    assertEquals(1, strategy.concurrencyKey(new PredicatedDeleteRecordMessage<>(stableClientId, 0, AlwaysTrue.alwaysTrue(), true)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void validConcurrencyKeyForUpdateMessages() {
    KeyConcurrencyStrategy<Integer> strategy = new KeyConcurrencyStrategy<>(new KeyHashCodeConcurrencyStrategy<Integer>(), mock(ReplicationConcurrencyStrategy.class));
    assertEquals(1, strategy.concurrencyKey(new PredicatedUpdateRecordMessage<>(stableClientId, 0, AlwaysTrue.alwaysTrue(),
            (IntrinsicUpdateOperation<Integer>) Functions.installUpdateOperation(Collections.emptySet()),
            false)));
  }

  @Test
  public void validConcurrencyKeyForAddMessages() {
    KeyConcurrencyStrategy<Integer> strategy = new KeyConcurrencyStrategy<>(new KeyHashCodeConcurrencyStrategy<Integer>(), mock(ReplicationConcurrencyStrategy.class));
    assertEquals(1, strategy.concurrencyKey(new AddRecordMessage<>(stableClientId, 0, Collections.emptyList(), true
    )));
  }

  @Test
  public void testValidConcurrencyKeyForStreamMessages() throws Exception {
    KeyConcurrencyStrategy<Integer> strategy = new KeyConcurrencyStrategy<>(new KeyHashCodeConcurrencyStrategy<Integer>(), mock(ReplicationConcurrencyStrategy.class));

    UUID streamId = UUID.randomUUID();
    int openConcurrencyKey = strategy.concurrencyKey(new PipelineProcessorOpenMessage(streamId, RemoteStreamType.INLINE,
        ElementType.RECORD, Collections.emptyList(), null, true));
    assertThat(openConcurrencyKey, is(allOf(greaterThan(0), lessThanOrEqualTo(65536))));
    assertThat(strategy.concurrencyKey(new PipelineProcessorCloseMessage(streamId)), is(openConcurrencyKey));
    assertThat(strategy.concurrencyKey(new TryAdvanceFetchMessage(streamId)), is(openConcurrencyKey));
    assertThat(strategy.concurrencyKey(new PipelineProcessorRequestResultMessage(streamId)), is(openConcurrencyKey));
    assertThat(strategy.concurrencyKey(new TryAdvanceReleaseMessage(streamId)), is(openConcurrencyKey));
  }
}
