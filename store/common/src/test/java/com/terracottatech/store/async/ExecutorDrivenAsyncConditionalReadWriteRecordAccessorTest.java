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

import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.UpdateOperation;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ExecutorDrivenAsyncConditionalReadWriteRecordAccessorTest extends AbstractExecutorDrivenAsyncTest {

  @Test
  public void testUpdate() throws ExecutionException, InterruptedException {
    @SuppressWarnings("unchecked")
    UpdateOperation<Long> operation = mock(UpdateOperation.class);
    testTerminal(a -> a.update(operation), a -> a.update(operation));
  }

  @Test
  public void testDelete() throws ExecutionException, InterruptedException {
    testTerminal(AsyncConditionalReadWriteRecordAccessor::delete, ConditionalReadWriteRecordAccessor::delete);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <T> void testTerminal(Function<AsyncConditionalReadWriteRecordAccessor<Long>, Operation<Optional<T>>> asyncMethod, Function<ConditionalReadWriteRecordAccessor<Long>, Optional> streamMethod) throws ExecutionException, InterruptedException {
    testDelegation(s -> new ExecutorDrivenAsyncConditionalReadWriteRecordAccessor<>(s, ForkJoinPool.commonPool()), Optional.class, asyncMethod.andThen(AbstractExecutorDrivenAsyncTest::retrieve),
            (Class<ConditionalReadWriteRecordAccessor<Long>>) (Class) ConditionalReadWriteRecordAccessor.class, Optional.class, streamMethod, (asyncOutput, delegateOutput) -> assertThat(asyncOutput, is(delegateOutput)));
  }
}
