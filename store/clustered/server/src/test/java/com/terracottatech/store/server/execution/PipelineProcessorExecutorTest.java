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
package com.terracottatech.store.server.execution;

import org.junit.Test;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class PipelineProcessorExecutorTest {

  @Test
  public void testPipelineProcessorExecutorExhaustion() throws Exception {
    final Semaphore semaphore = new Semaphore(0);
    final AtomicInteger threadsAllowed = new AtomicInteger(1);

    PipelineProcessorExecutor executor = new PipelineProcessorExecutor("test", 0, TimeUnit.SECONDS, threadsAllowed.get(), r -> {
      if (threadsAllowed.getAndUpdate(i -> max(0, i - 1)) > 0) {
        return new Thread(r);
      } else {
        throw new OutOfMemoryError("No more thread for you");
      }
    });
    try {
      Supplier<Runnable> taskSource = () -> semaphore::acquireUninterruptibly;

      try {
        executor.execute(taskSource.get());
        try {
          executor.execute(taskSource.get());
          fail("Expected RejectedExecutionException");
        } catch (RejectedExecutionException e) {
          //expected
        }
      } finally {
        semaphore.release();
      }
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

}
