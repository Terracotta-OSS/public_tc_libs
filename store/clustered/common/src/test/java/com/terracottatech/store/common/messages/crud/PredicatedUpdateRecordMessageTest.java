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

package com.terracottatech.store.common.messages.crud;

import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Record;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.internal.function.Functions;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.intrinsics.IntrinsicUpdateOperation;
import com.terracottatech.store.intrinsics.impl.AlwaysTrue;
import org.junit.Test;

import java.util.UUID;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PredicatedUpdateRecordMessageTest {
  private final UUID stableClientId = UUID.randomUUID();

  @Test
  public void hasPredicatedUpdateRecordMessageType() {
    IntrinsicPredicate<Record<?>> recordPredicate = AlwaysTrue.alwaysTrue();
    @SuppressWarnings("unchecked")
    IntrinsicUpdateOperation<String> updateOperation = (IntrinsicUpdateOperation<String>) Functions.installUpdateOperation(emptySet());
    PredicatedUpdateRecordMessage<String> message = new PredicatedUpdateRecordMessage<>(stableClientId, "key", recordPredicate, updateOperation, true);
    assertEquals(DatasetOperationMessageType.PREDICATED_UPDATE_RECORD_MESSAGE, message.getType());
    assertEquals(ChangeType.MUTATION, message.getChangeType());
    assertEquals(stableClientId, message.getStableClientId());
    assertSame(recordPredicate, message.getPredicate());
    assertSame(updateOperation, message.getUpdateOperation());
    assertEquals("key", message.getKey());
    assertTrue(message.isRespondInFull());
  }
}
