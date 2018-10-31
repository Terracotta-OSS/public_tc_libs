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

import com.terracottatech.store.stream.RecordStream;
import org.junit.Test;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ExecutorDrivenAsyncRecordStreamTest extends AbstractExecutorDrivenAsyncTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testExplain() {
    testIntermediate(AsyncRecordStream::explain, RecordStream::explain, Consumer.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFilter() {
    testIntermediate(AsyncRecordStream::filter, RecordStream::filter, Predicate.class);
  }

  @Test
  public void testDistinct() {
    testIntermediate(AsyncRecordStream::distinct, RecordStream::distinct);
  }

  @Test
  public void testSorted() {
    testIntermediate(AsyncRecordStream::sorted, RecordStream::sorted);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSortedWithComparator() {
    testIntermediate(AsyncRecordStream::sorted, RecordStream::sorted, Comparator.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPeek() {
    testIntermediate(AsyncRecordStream::peek, RecordStream::peek, Consumer.class);
  }

  @Test
  public void testLimit() {
    testIntermediate(AsyncRecordStream::limit, RecordStream::limit, Long.TYPE);
  }

  @Test
  public void testSkip() {
    testIntermediate(AsyncRecordStream::skip, RecordStream::skip, Long.TYPE);
  }

  @Test
  public void testSequential() {
    testIntermediate(AsyncRecordStream::sequential, RecordStream::sequential);
  }

  @Test
  public void testParallel() {
    testIntermediate(AsyncRecordStream::parallel, RecordStream::parallel);
  }

  @Test
  public void testUnordered() {
    testIntermediate(AsyncRecordStream::unordered, RecordStream::unordered);
  }

  @Test
  public void testOnClose() {
    testIntermediate(AsyncRecordStream::onClose, RecordStream::onClose, Runnable.class);
  }

  @SuppressWarnings("unchecked")
  private static void testIntermediate(Function<AsyncRecordStream<String>, AsyncRecordStream<String>> asyncMethod, Function<RecordStream<String>, RecordStream<String>> streamMethod) {
    Executor executor = mock(Executor.class);
    testDelegation(s -> new ExecutorDrivenAsyncRecordStream<String>(s, executor), ExecutorDrivenAsyncRecordStream.class, asyncMethod::apply,
            RecordStream.class, RecordStream.class, streamMethod::apply, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testIntermediate(BiFunction<AsyncRecordStream<String>, U, AsyncRecordStream<String>> asyncMethod, BiFunction<RecordStream<String>, U, RecordStream<String>> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }
}
