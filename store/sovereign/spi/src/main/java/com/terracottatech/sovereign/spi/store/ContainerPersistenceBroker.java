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
package com.terracottatech.sovereign.spi.store;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public interface ContainerPersistenceBroker {

  // need another durability type
  ContainerPersistenceBroker NOOP = new ContainerPersistenceBroker() {

    @Override
    public void close() {

    }

    @Override
    public void tapAdd(long key, ByteBuffer data) {

    }

    @Override
    public void tapDelete(long key, int oldDataSize) {

    }

    @Override
    public void tapReplace(long oldKey, int oldDataSize, long key, ByteBuffer data) {

    }

  };

  /**
   * Mark uneeded.
   */
  void close();

  /**
   * Called when an add happens for the related dataContainer.
   *  @param key
   * @param data
   */
  void tapAdd(long key, ByteBuffer data);

  /**
   * Called when a delete happens for the related data container
   * @param key
   * @param oldDataSize
   */
  void tapDelete(long key, int oldDataSize);

  /**
   * Replace one with another.
   * @param oldKey
   * @param oldDataSize
   * @param key
   * @param data
   */
  void tapReplace(long oldKey, int oldDataSize, long key, ByteBuffer data);

}
