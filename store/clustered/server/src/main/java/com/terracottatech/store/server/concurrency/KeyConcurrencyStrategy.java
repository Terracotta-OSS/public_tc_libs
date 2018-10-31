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

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.ManagementMessage;
import com.terracottatech.store.common.messages.crud.KeyMessage;
import com.terracottatech.store.server.messages.replication.ReplicationMessage;
import com.terracottatech.store.common.messages.stream.PipelineProcessorMessage;
import com.terracottatech.store.common.messages.UniversalMessage;
import org.terracotta.entity.ConcurrencyStrategy;

import java.util.Set;

public class KeyConcurrencyStrategy<K extends Comparable<K>> implements ConcurrencyStrategy<DatasetEntityMessage> {
  private final ConcurrencyStrategy<KeyMessage<K>> mutationConcurrencyStrategy;
  private final ReplicationConcurrencyStrategy replicationConcurrencyStrategy;
  private final PipelineProcessorConcurrencyStrategy pipelineProcessorConcurrencyStrategy = new PipelineProcessorConcurrencyStrategy();

  public KeyConcurrencyStrategy(ConcurrencyStrategy<KeyMessage<K>> mutationConcurrencyStrategy,
                                ReplicationConcurrencyStrategy replicationConcurrencyStrategy) {
    this.mutationConcurrencyStrategy = mutationConcurrencyStrategy;
    this.replicationConcurrencyStrategy = replicationConcurrencyStrategy;
  }

  @Override
  public int concurrencyKey(DatasetEntityMessage datasetEntityMessage) {
    if (datasetEntityMessage instanceof KeyMessage<?>) {
      @SuppressWarnings("unchecked")
      KeyMessage<K> keyMessage = (KeyMessage<K>) datasetEntityMessage;
      return mutationConcurrencyStrategy.concurrencyKey(keyMessage);
    } else if (datasetEntityMessage instanceof PipelineProcessorMessage) {
      return pipelineProcessorConcurrencyStrategy.concurrencyKey((PipelineProcessorMessage)datasetEntityMessage);
    } else if (datasetEntityMessage instanceof ReplicationMessage) {
      return replicationConcurrencyStrategy.concurrencyKey((ReplicationMessage) datasetEntityMessage);
    } else if (datasetEntityMessage instanceof UniversalMessage) {
      return UNIVERSAL_KEY;
    } else if (datasetEntityMessage instanceof ManagementMessage) {
      return MANAGEMENT_KEY;
    } else {
      throw new AssertionError("Illegal message type: " + datasetEntityMessage.getClass());
    }
  }

  @Override
  public Set<Integer> getKeysForSynchronization() {
    return mutationConcurrencyStrategy.getKeysForSynchronization();
  }
}
