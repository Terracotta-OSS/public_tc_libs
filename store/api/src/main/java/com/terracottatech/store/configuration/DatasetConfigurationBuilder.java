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

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.concurrent.TimeUnit;

public interface DatasetConfigurationBuilder {
  /**
   * Creates the DatasetConfiguration as defined by the builder state
   * @return a DatasetConfiguration
   */
  DatasetConfiguration build();

  /**
   * Set the name of the resource to use for offheap storage
   * @param resourceName the name of the resource to use
   * @return a DatasetConfigurationBuilder to continue with configuration
   */
  DatasetConfigurationBuilder offheap(String resourceName);

  /**
   * Set the name of the resource to use for disk storage and use/deploy the persistent storage engine configured as
   * system wide default.
   *
   * @param resourceName the name of the resource to use
   * @return a DatasetConfigurationBuilder to continue with configuration
   */
  DatasetConfigurationBuilder disk(String resourceName);

  /**
   * Set the name of the resource to use for disk storage and use/deploy the specified persistent storage engine type.
   *
   * @param resourceName the name of the disk resource to use
   * @param storageType the type of persistent storage engine to deploy/use
   * @return a DatasetConfigurationBuilder to continue with configuration
   */
  DatasetConfigurationBuilder disk(String resourceName, PersistentStorageType storageType);

  /**
   * Configure indexing for given cell.
   *
   * @param cellDefinition cell to index
   * @param settings index settings
   * @return a DatasetConfigurationBuilder to continue with configuration
   */
  DatasetConfigurationBuilder index(CellDefinition<?> cellDefinition, IndexSettings settings);

  /**
   * Configure this dataset to leave durability flushing to the operating
   * system. This is suitable in HA situations, where multiple servers
   * will store the values.
   *
   * Note that this is a constraint, and the implementation is free to be more
   * conservative.
   * @return a DatasetConfigurationBuilder to continue with configuration
   */
  DatasetConfigurationBuilder durabilityEventual();

  /**
   * Configure this dataset to forceable push every mutative operation to disk.
   * This is the safest approach, but has an obvious performance impact.
   *
   * Note that this is a constraint, and the implementation is free to be more
   * conservative.
   * @return a DatasetConfigurationBuilder to continue with configuration
   */
  DatasetConfigurationBuilder durabilityEveryMutation();

  /**
   * Configure this dataset to allow a mutative change to linger unflushed
   * to disk for no more than the specified duration.
   *
   * Note that this is a constraint, and the implementation is free to be more
   * conservative.
   * @return a DatasetConfigurationBuilder to continue with configuration
   */
  DatasetConfigurationBuilder durabilityTimed(long duration, TimeUnit units);

  /**
   * Access more advanced configuration options than are generally needed.
   */
  AdvancedDatasetConfigurationBuilder advanced();
}
