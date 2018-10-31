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

package com.terracottatech.store.common.dataset.stream;

import java.util.function.Consumer;

/**
 * Provides methods supporting unit testing.
 *
 * @author Clifford W. Johnson
 */
public class PackageSupport {
  private PackageSupport() {
  }

  /**
   * Makes package-private {@link PipelineMetaData#setPipelineConsumer(Consumer)} method available outside
   * this package for testing.
   *
   * @param metaData the {@code PipelineMetaData} instance to set
   * @param pipelineConsumer the {@code Consumer} to set
   */
  public static void setPipelineConsumer(final PipelineMetaData metaData, final Consumer<PipelineMetaData> pipelineConsumer) {
    metaData.setPipelineConsumer(pipelineConsumer);
  }
}
