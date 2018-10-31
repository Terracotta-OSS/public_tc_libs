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
package com.terracottatech.store.server.storage.configuration;

import com.tc.classloader.CommonComponent;
import com.terracottatech.store.configuration.PersistentStorageType;

@CommonComponent
public class PersistentStorageConfiguration extends StorageConfiguration {
  private final String diskResource;
  private final PersistentStorageType persistentStorageType;

  /**
   * @param diskResource the name of the data directory resource in which files are to be stored - may not be null
   * @param offheapResource the name of the offheap resource - may not be null
   */
  public PersistentStorageConfiguration(String diskResource, String offheapResource) {
    super(StorageType.FRS, offheapResource);
    this.diskResource = diskResource;
    this.persistentStorageType = PersistentStorageType.defaultEngine();
  }

  /**
   * @param diskResource the name of the data directory resource in which files are to be stored - may not be null
   * @param offheapResource the name of the offheap resource - may not be null
   * @param storageType the type of storage technology to use for this dataset - may not be null
   */
  public PersistentStorageConfiguration(String diskResource, String offheapResource, PersistentStorageType storageType) {
    super(StorageType.toPersistentStorageType(storageType), offheapResource);
    this.diskResource = diskResource;
    this.persistentStorageType = storageType;
  }

  public String getDiskResource() {
    return diskResource;
  }

  public PersistentStorageType getPersistentStorageType() {
    return persistentStorageType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof PersistentStorageConfiguration)) {
      return false;
    }

    if (!super.equals(o)) {
      return false;
    }

    PersistentStorageConfiguration that = (PersistentStorageConfiguration) o;

    return diskResource.equals(that.diskResource) &&
           getPersistentStorageType().getPermanentId() == that.getPersistentStorageType().getPermanentId();
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + diskResource.hashCode();
    result = 31 * result + persistentStorageType.hashCode();
    return result;
  }
}
