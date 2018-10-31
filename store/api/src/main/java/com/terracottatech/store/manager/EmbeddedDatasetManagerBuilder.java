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

package com.terracottatech.store.manager;

import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.configuration.PersistentStorageEngine;
import com.terracottatech.store.configuration.PersistentStorageType;

import java.nio.file.Path;

/**
 * An EmbeddedDatasetManagerBuilder allows configuration of the interaction with embedded storage.
 */
public interface EmbeddedDatasetManagerBuilder {
  /**
   * Creates the DatasetManager that has been configured using this EmbeddedDatasetManagerBuilder
   *
   * @return a DatasetManager instance
   * @throws StoreException if using a disk resource that fails or using multiple disk resources that contain
   * duplicated Dataset names
   */
  DatasetManager build() throws StoreException;

  /**
   * Registers an amount of offheap memory
   *
   * @param resourceName the name that can later be used to refer to this memory
   * @param unitCount the number of memory units
   * @param memoryUnit the unit of memory
   * @return a EmbeddedDatasetManagerBuilder to allow further configuration
   */
  EmbeddedDatasetManagerBuilder offheap(String resourceName, long unitCount, MemoryUnit memoryUnit);

  /**
   * Registers a location for local disk storage.
   * <p>
   *   NOTE: This API is deprecated and is here ONLY for backward compatibility purposes. Please use
   *   {@link this#disk(String, Path, FileMode)} instead.
   * </p>
   *
   * @param resourceName the name that can later be used to refer to this location
   * @param dataRootDirectory the location on local disk for storage
   * @param persistenceMode persistence mode used for this location
   * @param fileMode file open mode used for this location
   * @return a EmbeddedDatasetManagerBuilder to allow further configuration
   */
  EmbeddedDatasetManagerBuilder disk(String resourceName, Path dataRootDirectory, PersistenceMode persistenceMode, FileMode fileMode);

  /**
   * Registers a location for local disk storage.
   *
   * @param resourceName the name that can later be used to refer to this location
   * @param dataRootDirectory the location on local disk for storage
   * @param fileMode file open mode used for this location
   * @return a EmbeddedDatasetManagerBuilder to allow further configuration
   */
  EmbeddedDatasetManagerBuilder disk(String resourceName, Path dataRootDirectory, FileMode fileMode);

  enum FileMode {
    /**
     * Instantiate a fresh persistent storage instance. If an instance exists already the operation will fail.
     */
    NEW,

    /**
     * Reopen an existing instance. If the directory is empty the operation will fail.
     */
    REOPEN,

    /**
     * Destroy the existing instance, if one is found, and create a new one.
     */
    OVERWRITE,

    /**
     * Reopen the existing instance, if one is found, or create a new one.
     */
    REOPEN_OR_NEW
  }

  enum PersistenceMode {
    /**
     * All data is stored in-memory, disk is used purely for persistence.
     */
    INMEMORY(PersistentStorageEngine.FRS),

    /**
     * Some data is stored in-memory.  Read operations may be served by either
     * disk or memory depending on the read in question.  This mode of operation
     * allows for the storage of more data than the configured offheap size.
     */
    HYBRID(PersistentStorageEngine.HYBRID),

    /**
     * The storage type specification is delegated to the dataset API for possibility
     * of richer specification of storage types and to bring consistency between the clustered and
     * embedded APIs.
     */
    DELEGATED(null);

    private final PersistentStorageType storageType;

    PersistenceMode(PersistentStorageType storageType) {
      this.storageType = storageType;
    }

    public PersistentStorageType getStorageType() {
      return this.storageType;
    }
  }
}
