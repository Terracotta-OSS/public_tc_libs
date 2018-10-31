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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ExecutionServiceImpl implements ExecutionService {

  private final List<Map.Entry<Pattern, ThreadPoolExecutor>> unorderedExecutors;

  public ExecutionServiceImpl(List<Map.Entry<Pattern, ThreadPoolExecutor>> unorderedExecutors) {
    this.unorderedExecutors = Objects.requireNonNull(unorderedExecutors, "unorderedExecutors must not be null");
  }

  @Override
  public ExecutorService getUnorderedExecutor(String poolAlias) {
    for (Map.Entry<Pattern, ThreadPoolExecutor> pool : unorderedExecutors) {
      if (pool.getKey().matcher(poolAlias).matches()) {
        ThreadPoolExecutor threadPoolExecutor = pool.getValue();
        return new PartitionedUnorderedExecutor(new LinkedBlockingQueue<>(), threadPoolExecutor,  threadPoolExecutor.getMaximumPoolSize());
      }
    }
    throw new IllegalArgumentException("No configured thread pool matches '" + poolAlias + "'");
  }

  @Override
  public ExecutorService getPipelineProcessorExecutor(String poolAlias) {
    return new PipelineProcessorExecutor(poolAlias, 500, TimeUnit.MILLISECONDS, 128);
  }

  public void close() {
    for (Map.Entry<Pattern, ThreadPoolExecutor> unorderedExecutor : unorderedExecutors) {
      unorderedExecutor.getValue().shutdownNow();
    }
  }

}
