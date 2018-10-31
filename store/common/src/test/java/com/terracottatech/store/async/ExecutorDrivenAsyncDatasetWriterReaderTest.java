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

import com.terracottatech.store.Cell;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

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
public class ExecutorDrivenAsyncDatasetWriterReaderTest {

  @Test
  public void testAddDelegatesCorrectly() throws InterruptedException, ExecutionException {
    Collection<Cell<?>> cells = mock(Collection.class);
    DatasetWriterReader<Long> access = mock(DatasetWriterReader.class);
    when(access.add(1L, cells)).thenReturn(true);

    ExecutorDrivenAsyncDatasetWriterReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetWriterReader<>(access, ForkJoinPool.commonPool());

    assertThat(asyncAccess.add(1L, cells).get(), is(true));
    verify(access).add(1L, cells);
  }

  @Test
  public void testUpdateDelegatesCorrectly() throws InterruptedException, ExecutionException {
    UpdateOperation<Long> operation = mock(UpdateOperation.class);
    DatasetWriterReader<Long> access = mock(DatasetWriterReader.class);
    when(access.update(1L, operation)).thenReturn(true);

    ExecutorDrivenAsyncDatasetWriterReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetWriterReader<>(access, ForkJoinPool.commonPool());

    assertThat(asyncAccess.update(1L, operation).get(), is(true));
    verify(access).update(1L, operation);
  }

  @Test
  public void testDeleteDelegatesCorrectly() throws InterruptedException, ExecutionException {
    DatasetWriterReader<Long> access = mock(DatasetWriterReader.class);
    when(access.delete(1L)).thenReturn(true);

    ExecutorDrivenAsyncDatasetWriterReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetWriterReader<>(access, ForkJoinPool.commonPool());

    assertThat(asyncAccess.delete(1L).get(), is(true));
    verify(access).delete(1L);
  }

  @Test
  public void testRecordsDelegatesCorrectly() throws InterruptedException, ExecutionException {
    MutableRecordStream<Long> stream = mock(MutableRecordStream.class);
    DatasetWriterReader<Long> access = mock(DatasetWriterReader.class);
    when(access.records()).thenReturn(stream);

    ExecutorDrivenAsyncDatasetWriterReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetWriterReader<>(access, ForkJoinPool.commonPool());

    asyncAccess.records();

    verify(access).records();
  }

  @Test
  public void testOnDelegatesCorrectly() throws InterruptedException, ExecutionException {
    MutableRecordStream<Long> stream = mock(MutableRecordStream.class);
    DatasetWriterReader<Long> access = mock(DatasetWriterReader.class);
    when(access.records()).thenReturn(stream);

    ExecutorDrivenAsyncDatasetWriterReader<Long> asyncAccess = new ExecutorDrivenAsyncDatasetWriterReader<>(access, ForkJoinPool.commonPool());

    AsyncReadWriteRecordAccessor<Long> on = asyncAccess.on(1L);

    verify(access).on(1L);
  }
}
