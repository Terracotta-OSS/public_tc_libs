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
package com.terracottatech.store.async;

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.ReadRecordAccessor;
import com.terracottatech.store.Record;

import com.terracottatech.store.stream.RecordStream;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author cdennis
 */
@SuppressWarnings("unchecked")
public class ExecutorDrivenAsyncDatasetReaderTest {

  @Test
  public void testGetDelegatesCorrectly() throws InterruptedException, ExecutionException {
    Record<Long> record = mock(Record.class);
    DatasetReader<Long> access = mock(DatasetReader.class);
    when(access.get(1L)).thenReturn(of(record));

    ExecutorDrivenAsyncDatasetReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetReader<>(access, ForkJoinPool.commonPool());

    assertThat(asyncAccess.get(1L).get().get(), is(record));
  }

  @Test
  public void testRecordsDelegatesCorrectly() throws InterruptedException, ExecutionException {
    RecordStream<Long> stream = mock(RecordStream.class);
    DatasetReader<Long> access = mock(DatasetReader.class);
    when(access.records()).thenReturn(stream);

    ExecutorDrivenAsyncDatasetReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetReader<>(access, ForkJoinPool.commonPool());

    asyncAccess.records();

    verify(access).records();
  }

  @Test
  public void testOnDelegatesCorrectly() throws InterruptedException, ExecutionException {
    ReadRecordAccessor<Long> keyAccessor = mock(ReadRecordAccessor.class);
    DatasetReader<Long> access = mock(DatasetReader.class);
    when(access.on(1L)).thenReturn(keyAccessor);

    ExecutorDrivenAsyncDatasetReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetReader<>(access, ForkJoinPool.commonPool());

    AsyncReadRecordAccessor<Long> on = asyncAccess.on(1L);

    verify(access).on(1L);
  }
}
