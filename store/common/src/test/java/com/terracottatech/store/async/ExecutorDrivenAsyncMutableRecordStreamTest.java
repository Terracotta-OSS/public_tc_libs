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

import com.terracottatech.store.stream.MutableRecordStream;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ExecutorDrivenAsyncMutableRecordStreamTest extends AbstractExecutorDrivenAsyncTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testExplain() {
    testIntermediate(AsyncMutableRecordStream::explain, MutableRecordStream::explain, Consumer.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFilter() {
    testIntermediate(AsyncMutableRecordStream::filter, MutableRecordStream::filter, Predicate.class);
  }

  @Test
  public void testDistinct() {
    testIntermediate(AsyncMutableRecordStream::distinct, MutableRecordStream::distinct);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPeek() {
    testIntermediate(AsyncMutableRecordStream::peek, MutableRecordStream::peek, Consumer.class);
  }

  @Test
  public void testLimit() {
    testIntermediate(AsyncMutableRecordStream::limit, MutableRecordStream::limit, Long.TYPE);
  }

  @Test
  public void testSkip() {
    testIntermediate(AsyncMutableRecordStream::skip, MutableRecordStream::skip, Long.TYPE);
  }

  @Test
  public void testSequential() {
    testIntermediate(AsyncMutableRecordStream::sequential, MutableRecordStream::sequential);
  }

  @Test
  public void testParallel() {
    testIntermediate(AsyncMutableRecordStream::parallel, MutableRecordStream::parallel);
  }

  @Test
  public void testUnordered() {
    testIntermediate(AsyncMutableRecordStream::unordered, MutableRecordStream::unordered);
  }

  @Test
  public void testOnClose() {
    testIntermediate(AsyncMutableRecordStream::onClose, MutableRecordStream::onClose, Runnable.class);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static void testIntermediate(Function<AsyncMutableRecordStream<String>, AsyncMutableRecordStream<String>> asyncMethod, Function<MutableRecordStream<String>, MutableRecordStream<String>> streamMethod) {
    Executor executor = mock(Executor.class);
    Function<MutableRecordStream, ExecutorDrivenAsyncMutableRecordStream<String>> function = s -> new ExecutorDrivenAsyncMutableRecordStream<String>(s, executor);
    testDelegation(function, ExecutorDrivenAsyncMutableRecordStream.class, asyncMethod::apply,
            MutableRecordStream.class, MutableRecordStream.class, streamMethod::apply, (asyncOutput, delegateOutput) -> {
              assertThat(asyncOutput.getExecutor(), is(executor));
              assertThat(asyncOutput.getStream(), is(delegateOutput));
            });
  }

  private static <U> void testIntermediate(BiFunction<AsyncMutableRecordStream<String>, U, AsyncMutableRecordStream<String>> asyncMethod, BiFunction<MutableRecordStream<String>, U, MutableRecordStream<String>> streamMethod, Class<U> paramType) {
    U parameter = createInstance(paramType);
    testIntermediate(as -> asyncMethod.apply(as, parameter), s -> streamMethod.apply(s, parameter));
  }
}
