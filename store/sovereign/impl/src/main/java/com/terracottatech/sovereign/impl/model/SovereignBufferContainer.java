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
package com.terracottatech.sovereign.impl.model;

import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;

import java.nio.ByteBuffer;

/**
 * This class, particularly, expects to be used under write lock. The Record Container classes
 * that compose on top of this, need to ensure proper lock scoping.
 *
 * @author cschanck
 **/
public interface SovereignBufferContainer {

  int getShardIndex();

  SovereignRuntime<?> runtime();

  PersistentMemoryLocator add(ByteBuffer data);

  PersistentMemoryLocator reinstall(long lsn, long persistentKey, ByteBuffer data);

  PersistentMemoryLocator restore(long key, ByteBuffer data, ByteBuffer oldData);

  void deleteIfPresent(long key);

  boolean delete(PersistentMemoryLocator key);

  PersistentMemoryLocator replace(PersistentMemoryLocator priorKey, ByteBuffer data);

  ByteBuffer get(PersistentMemoryLocator key);

  PersistentMemoryLocator first(ContextImpl context);

  void dispose();

  long count();

}
