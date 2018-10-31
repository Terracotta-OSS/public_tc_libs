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
package com.terracottatech.store.configuration;

/**
 * Interface for advanced {@link com.terracottatech.store.Dataset dataset} configuration options.
 * <p>
 * This interface defines advanced configuration options that most users will not require.
 * If you find your way here, make sure you understand what you are tweaking and that some of these tweaks may not
 * have the same impact across versions.
 */
public interface AdvancedDatasetConfigurationBuilder extends DatasetConfigurationBuilder {

  /**
   * Gives a hint to the desired level of concurrency.
   * <p>
   * By default, a {@link com.terracottatech.store.Dataset dataset} supports parallel operations, including writes.
   * As the number of write operations increases, contention among write operations may
   * degrade performance. Increasing the concurrency hint may improve performance of write-heavy
   * loads by increasing the support for parallel operations. This increase in concurrency
   * comes at the cost of additional memory overhead. Increasing the concurrency
   * hint will <b>not</b> improve the performance of all workloads -- if the write
   * activity is primarily against a limited number of records, for example
   * against one record, then increasing the concurrency level will not help.
   * <p>
   * The default concurrency of a dataset is based on hardware heuristics and should fit common usage.
   * Changing the value is considered an advanced tuning and should imply a corresponding benchmark.
   * <p>
   * To reduce overhead, if a dataset is read-only or has a limited number of writes,
   * you might also want to reduce the concurrency level.
   * <p>
   * Once a dataset is created with a given value, it cannot be changed without having to re-create the dataset.
   *
   * @param hint the concurrency hint
   * @return a {@code DatasetConfigurationBuilder} to continue with configuration
   */
  AdvancedDatasetConfigurationBuilder concurrencyHint(int hint);

}
