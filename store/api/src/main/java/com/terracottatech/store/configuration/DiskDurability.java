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
 * Used to specify the disk durability of a dataset. Users will not
 * interact with this class directly, it wil be leveraged by the
 * configuration builders.
 */
public interface DiskDurability {

  /**
   * Order is from most conservative to least conservative. This matters for
   * discerning the most conservative durability in a set of them.
   */
  enum DiskDurabilityEnum {
    EVERY_MUTATION,
    TIMED,
    EVENTUAL;
  }

  DiskDurabilityEnum getDurabilityEnum();

  DiskDurability mergeConservatively(DiskDurability other);
}
