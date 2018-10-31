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

import com.terracottatech.store.ConditionalReadRecordAccessor;
import com.terracottatech.store.ReadRecordAccessor;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ExecutorDrivenAsyncReadRecordAccessorTest extends AbstractExecutorDrivenAsyncTest {

  @Test
  public void testIff() {
    Executor executor = mock(Executor.class);
    Predicate predicate = mock(Predicate.class);
    testDelegation(s -> new ExecutorDrivenAsyncReadRecordAccessor<>(s, executor), ExecutorDrivenAsyncConditionalReadRecordAccessor.class, a -> a.iff(predicate),
            ReadRecordAccessor.class, ConditionalReadRecordAccessor.class, a -> a.iff(predicate), (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getDelegate(), is(delegateOutput));
            });
  }

  @Test
  public void testRead() {
    Function mapper = mock(Function.class);
    testDelegation(s -> new ExecutorDrivenAsyncReadRecordAccessor<>(s, ForkJoinPool.commonPool()), Optional.class, a -> retrieve(a.read(mapper)),
            ReadRecordAccessor.class, Optional.class, a -> a.read(mapper), (asyncOutput, delegateOutput) -> assertThat(asyncOutput, is(delegateOutput)));
  }
}
