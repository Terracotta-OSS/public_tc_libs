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
package com.terracottatech.sovereign.btrees.stores.disk;

import java.io.IOException;

/**
 * The block buffer which is evictable to some other secondary storage.
 */
public interface EvictableBlockBuffer extends BlockBuffer {

  /**
   * Is it currently loaded.
   *
   * @return
   */
  boolean isLoaded();

  /**
   * Gets age.
   *
   * @return the age
   */
  long getAge();

  /**
   * Fault it in until evicted.
   *
   * @throws IOException
   */
  void pin() throws IOException;

  /**
   * Evict this buffer, writing its contents as needed.
   *
   * @throws IOException
   */
  void evict() throws IOException;

  /**
   * Gets reference count.
   *
   * @return the reference count
   */
  int getReferenceCount();

  /**
   * Sets reference count.
   *
   * @param cnt the cnt
   */
  void setReferenceCount(int cnt);

  /**
   * Gets mod count.
   *
   * @return the mod count
   */
  int getModCount();
}
