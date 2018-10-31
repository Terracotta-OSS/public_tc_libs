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
package com.terracottatech.sovereign;

import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.sovereign.description.SovereignIndexDescription;
import com.terracottatech.sovereign.time.TimeReference;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Storage object, providing single point of storage for multiple DataSets.
 *
 * @author cschanck
 **/
public interface SovereignStorage<DS extends SovereignDataset<?>, DSS extends SovereignDatasetDescription> {
  /**
   * Get the buffer resource for this storage object
   * @return
   */
  SovereignBufferResource getBufferResource();

  /**
   * Restart the meta data for this storage object.
   * @return
   * @throws IOException
   */
  Future<Void> startupMetadata() throws IOException;

  /**
   * Restart the data for this storage object.
   * @return
   * @throws IOException
   */
  <Z extends TimeReference<Z>> Future<Void> startupData() throws IOException;

  /**
   * Gets indexes for a specific dataset, identified by uuid.
   *
   * @param uuid the uuid
   * @return the indexes
   */
  Collection<SovereignIndexDescription<?>> getIndexes(UUID uuid);

  /**
   * Shutdown all datasets related to this storage.
   *
   * @throws IOException
   */
  void shutdown() throws IOException;

  /**
   * Gets all managed datasets.
   *
   * @return the managed datasets
   */
  Collection<DS> getManagedDatasets();

  /**
   * Gets a specific dataset by uuid.
   *
   * @param uuid the uuid
   * @return the dataset
   */
  DS getDataset(UUID uuid);

  /**
   * Gets data set descriptions.
   *
   * @return the data set descriptions
   */
  Collection<DSS> getDataSetDescriptions();

  /**
   * Destroy the specified dataset.
   *
   * @param uuid
   */
  void destroyDataSet(UUID uuid) throws IOException;

  /**
   * Is this storage backend persistent.
   * @return
   */
  boolean isPersistent();

}
