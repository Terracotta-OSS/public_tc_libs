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
import com.terracottatech.sovereign.spi.IndexMapConfig;
import com.terracottatech.sovereign.spi.Space;

/**
 * @author cschanck
 **/
public interface SovereignSpace extends Space<ContextImpl, PersistentMemoryLocator> {

  @Override
  SovereignRuntime<?> runtime();

  @Override
  <K extends Comparable<K>> SovereignIndexMap<K, ?> createMap(String purpose, IndexMapConfig<K> config);

}
