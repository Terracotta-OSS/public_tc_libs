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

import com.tc.classloader.CommonComponent;
import com.terracottatech.store.server.stream.PipelineProcessor;

import java.util.concurrent.ExecutorService;

@CommonComponent
public interface ExecutionService {

  /**
   * Get an {@link ExecutorService} that does not give any guarantee about order of execution.
   * <p>
   * Every different pool alias may or may not return a different {@link ExecutorService}. The caller is
   * responsible for lifecycling the returned {@link ExecutorService}.
   *
   * @param poolAlias a hint about what {@link ExecutorService} should be returned.
   * @return an {@link ExecutorService}.
   */
  ExecutorService getUnorderedExecutor(String poolAlias);

  /**
   * Get an {@link ExecutorService} that gives special guarantees
   * an {@link PipelineProcessor} requires.
   * <p>
   * Every different pool alias will return a different {@link ExecutorService}. The caller is
   * responsible for lifecycling the returned {@link ExecutorService}.
   *
   * @param poolAlias a hint about what pipeline processor asks for the {@link ExecutorService}.
   * @return an {@link ExecutorService}.
   */
  ExecutorService getPipelineProcessorExecutor(String poolAlias);

}
