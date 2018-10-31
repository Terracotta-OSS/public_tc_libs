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
 * Persistent Storage engine type configured for a given dataset along with its attributes.
 */
public interface PersistentStorageType {
  int FRS_PERMANENT_ID = 1;
  int HYBRID_PERMANENT_ID = 2;

  /**
   * Returns a display name for this persistent storage type that cen be used for logging etc.
   *
   * @return a display name
   */
  String getLongName();

  /**
   * Returns a case insensitive name for this persistent storage type that can be used for configuration etc.
   *
   * @return a short name that can be used to match with config, if any
   */
  String getShortName();

  /**
   * Returns the status of the current feature support for this particular persistent storage engine.
   *
   * @return status of this product feature
   */
  ProductFeatureStatus getEngineStatus();

  /**
   * Returns a permanent identifier for this persistent storage type that is guaranteed to stay unique, regardless of
   * all the new persistent storage type(s) that may be added in the future.
   *
   * @return a permanent identifier for this persistent storage type
   */
  int getPermanentId();

  /**
   * System wide default persistent storage engine type used, if none is specified for a dataset.
   *
   * @return default persistent storage engine type
   */
  static PersistentStorageType defaultEngine() {
    return PersistentStorageEngine.FRS;
  }

  static PersistentStorageType permanentIdToStorageType(int storageId) {
    switch (storageId) {
      case FRS_PERMANENT_ID:
        return PersistentStorageEngine.FRS;
      case HYBRID_PERMANENT_ID:
        return PersistentStorageEngine.HYBRID;
      default:
        return null;
    }
  }

  static PersistentStorageType shortNameToType(String name) {
    // currently there are only enums for storage types
    try {
      return PersistentStorageEngine.valueOf(name);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Storage Type " + name, e);
    }
  }
}