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
package com.terracottatech.sovereign.indexing;

public interface SovereignSortedIndexStatistics extends SovereignIndexStatistics {

  /**
   * Number of times the index has been accessed from the first element to
   * root an iteration.
   *
   * @return
   */
  long indexFirstCount();

  /**
   * Number of times the index has been accessed from the last element to
   * root an iteration.
   *
   * @return
   */
  long indexLastCount();

  /**
   * Number of times the index has been accessed for a specific element
   *
   * @return
   */
  long indexGetCount();

  /**
   * Number of times the index has been accessed for a greater than element to
   * root an iteration.
   *
   * @return
   */
  long indexHigherCount();

  /**
   * Number of times the index has been accessed for a greater than or equal
   * element to root an iteration.
   *
   * @return
   */
  long indexHigherEqualCount();

  /**
   * Number of times the index has been accessed for a less than
   * element to root an iteration.
   *
   * @return
   */
  long indexLowerCount();

  /**
   * Number of times the index has been accessed for a less than or equal
   * element to root an iteration.
   *
   * @return
   */
  long indexLowerEqualCount();

}
