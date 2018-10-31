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

import com.terracottatech.store.server.messages.replication.DataReplicationMessage;
import com.terracottatech.store.server.messages.replication.MetadataReplicationMessage;
import com.terracottatech.store.Type;
import org.junit.Test;
import org.terracotta.entity.ConcurrencyStrategy;

import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicationConcurrencyStrategyTest {
  @Test
  public void concurrencyKey() throws Exception {
    MetadataReplicationMessage metadataReplicationMessage = mock(MetadataReplicationMessage.class);
    DataReplicationMessage dataReplicationMessage = mock(DataReplicationMessage.class);

    when(dataReplicationMessage.getIndex()).thenReturn(10000L);

    ConcurrencyShardMapper concurrencyShardMapper = new ConcurrencyShardMapper(Type.LONG, Optional.of(1));
    ReplicationConcurrencyStrategy replicationConcurrencyStrategy = new ReplicationConcurrencyStrategy(concurrencyShardMapper);

    assertThat(replicationConcurrencyStrategy.concurrencyKey(metadataReplicationMessage), is(ConcurrencyStrategy.MANAGEMENT_KEY));
    assertThat(replicationConcurrencyStrategy.concurrencyKey(dataReplicationMessage), is(1));
  }

}