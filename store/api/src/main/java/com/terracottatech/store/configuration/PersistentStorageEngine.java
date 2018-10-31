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

import static com.terracottatech.store.configuration.ProductFeatureStatus.*;

/**
 * Programmatic representation of various persistent storage engine supported by the system and their attributes.
 */
public enum PersistentStorageEngine implements PersistentStorageType {
  FRS(FRS_PERMANENT_ID, SUPPORTED, "Fast Restart Store"),
  HYBRID(HYBRID_PERMANENT_ID, SUPPORTED, "Hybrid Store");

  private final int identifier;
  private final ProductFeatureStatus engineStatus;
  private final String longName;

  PersistentStorageEngine(int id, ProductFeatureStatus engineStatus, String longName) {
    this.identifier = id;
    this.engineStatus = engineStatus;
    this.longName = longName;
  }

  @Override
  public String getLongName() {
    return longName;
  }

  @Override
  public String getShortName() {
    return this.name();
  }

  @Override
  public ProductFeatureStatus getEngineStatus() {
    return engineStatus;
  }

  @Override
  public int getPermanentId() {
    return identifier;
  }
}
