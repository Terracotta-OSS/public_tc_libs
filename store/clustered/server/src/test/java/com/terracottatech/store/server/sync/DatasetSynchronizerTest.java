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
package com.terracottatech.store.server.sync;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.sovereign.impl.memory.RecordContainerChangeListener.ChangeListener;
import com.terracottatech.sovereign.impl.memory.ShardIterator;
import com.terracottatech.sovereign.impl.memory.ShardedRecordContainer;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.server.messages.replication.BatchedDataSyncMessage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.terracotta.entity.PassiveSynchronizationChannel;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class DatasetSynchronizerTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testBatching() {
    SovereignDatasetImpl<Integer> dataset = mock(SovereignDatasetImpl.class);
    Supplier<SovereignDataset<Integer>> datasetSupplier = () -> dataset;
    DatasetSynchronizer<Integer> datasetSynchronizer = new DatasetSynchronizer<>(datasetSupplier);

    ShardIterator shardIterator = mock(ShardIterator.class);
    when(dataset.iterate(0)).thenReturn(shardIterator);
    when(dataset.iterate(1)).thenReturn(shardIterator);

    BufferDataTuple bufferDataTuple = mock(BufferDataTuple.class);

    Long[] indexes = new Long[19];

    for (int i = 0; i < 19; i++) {
      indexes[i] = (long)(i + 2);
    }

    when(bufferDataTuple.index()).thenReturn(1L, indexes);
    when(bufferDataTuple.getData()).thenReturn(ByteBuffer.allocate(1000 * 1000));

    Boolean[] hasnexts = new Boolean[20];
    for (int i = 0; i < 19; i++) {
      hasnexts[i] = true;
    }
    hasnexts[19] = false;

    when(shardIterator.hasNext()).thenReturn(true, hasnexts);
    when(shardIterator.next()).thenReturn(bufferDataTuple);

    PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel = mock(PassiveSynchronizationChannel.class);

    ArgumentCaptor<DatasetEntityMessage> argumentCaptor = ArgumentCaptor.forClass(DatasetEntityMessage.class);

    datasetSynchronizer.synchronizeShard(synchronizationChannel, 0);
    datasetSynchronizer.synchronizeShard(synchronizationChannel, 1);

    verify(synchronizationChannel, times(5)).synchronizeToPassive(argumentCaptor.capture());

    assertThat(argumentCaptor.getAllValues().size(), is(5));

    assertThat(argumentCaptor.getAllValues().stream()
            .mapToInt(x -> ((BatchedDataSyncMessage) x).getDataTuples().size())
            .sum(), is(20));
  }

  @Test
  public void testSynchronizeShard() {
    SovereignDatasetImpl<Integer> dataset = mock(SovereignDatasetImpl.class);
    Supplier<SovereignDataset<Integer>> datasetSupplier = () -> dataset;
    DatasetSynchronizer<Integer> datasetSynchronizer = new DatasetSynchronizer<>(datasetSupplier);

    PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel = mock(PassiveSynchronizationChannel.class);
    PassiveSynchronizationChannel<DatasetEntityMessage> altSynchronizationChannel = mock(PassiveSynchronizationChannel.class);

    ShardIterator shardIterator = mock(ShardIterator.class);
    when(dataset.iterate(0)).thenReturn(shardIterator);
    when(dataset.iterate(1)).thenReturn(shardIterator);

    datasetSynchronizer.synchronizeShard(synchronizationChannel, 0);
    datasetSynchronizer.synchronizeShard(synchronizationChannel, 1);

    datasetSynchronizer.synchronizeShard(altSynchronizationChannel, 0);
    datasetSynchronizer.synchronizeShard(altSynchronizationChannel, 1);

    verify(dataset, times(1)).addChangeListener(0, datasetSynchronizer.getOnGoingPassiveSyncs().get(synchronizationChannel));
    verify(dataset, times(1)).addChangeListener(1, datasetSynchronizer.getOnGoingPassiveSyncs().get(synchronizationChannel));
    verify(dataset, times(1)).addChangeListener(0, datasetSynchronizer.getOnGoingPassiveSyncs().get(altSynchronizationChannel));
    verify(dataset, times(1)).addChangeListener(1, datasetSynchronizer.getOnGoingPassiveSyncs().get(altSynchronizationChannel));
    verify(dataset, times(2)).iterate(0);
    verify(dataset, times(2)).iterate(1);
  }

  @Test
  public void testSynchronizeRecordedMutationFailsWhenSyncChannelNotSame() {

    SovereignDatasetImpl<Integer> dataset = mock(SovereignDatasetImpl.class);
    Supplier<SovereignDataset<Integer>> datasetSupplier = () -> dataset;
    DatasetSynchronizer<Integer> datasetSynchronizer = new DatasetSynchronizer<>(datasetSupplier);

    PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel = mock(PassiveSynchronizationChannel.class);

    expectedException.expect(IllegalStateException.class);
    datasetSynchronizer.synchronizeRecordedMutations(synchronizationChannel, 0);

  }

  @Test
  public void testSynchronizeRecordedMutation() {
    SovereignDatasetImpl<Integer> dataset = mock(SovereignDatasetImpl.class);
    Supplier<SovereignDataset<Integer>> datasetSupplier = () -> dataset;
    DatasetSynchronizer<Integer> datasetSynchronizer = new DatasetSynchronizer<>(datasetSupplier);

    PassiveSynchronizationChannel<DatasetEntityMessage> synchronizationChannel = mock(PassiveSynchronizationChannel.class);
    PassiveSynchronizationChannel<DatasetEntityMessage> altSynchronizationChannel = mock(PassiveSynchronizationChannel.class);

    ShardIterator shardIterator = mock(ShardIterator.class);
    when(dataset.iterate(0)).thenReturn(shardIterator);
    when(dataset.iterate(1)).thenReturn(shardIterator);

    @SuppressWarnings("rawtypes")
    ShardedRecordContainer container = mock(ShardedRecordContainer.class);
    when(dataset.getContainer()).thenReturn(container);
    List<?> list = mock(List.class);
    when(container.getShards()).thenReturn(list);
    when(list.size()).thenReturn(2);

    datasetSynchronizer.synchronizeShard(synchronizationChannel, 0);
    datasetSynchronizer.synchronizeRecordedMutations(synchronizationChannel, 0);
    datasetSynchronizer.synchronizeShard(synchronizationChannel, 1);
    datasetSynchronizer.synchronizeRecordedMutations(synchronizationChannel, 1);

    datasetSynchronizer.synchronizeShard(altSynchronizationChannel, 0);
    datasetSynchronizer.synchronizeRecordedMutations(altSynchronizationChannel, 0);
    datasetSynchronizer.synchronizeShard(altSynchronizationChannel, 1);
    datasetSynchronizer.synchronizeRecordedMutations(altSynchronizationChannel, 1);

    verify(dataset, times(4)).consumeMutationsThenRemoveListener(anyInt(), any(ChangeListener.class), any());
  }

}