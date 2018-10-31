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
package com.terracottatech.sovereign.impl.memory.storageengines;

import org.terracotta.offheapstore.storage.portability.Portability;

import java.nio.ByteBuffer;

/**
 * Created by cschanck on 5/4/2016.
 */
public interface PrimitivePortability<T> extends Portability<T> {
  boolean isInstance(Object o);

  void encode(T obj, ByteBuffer dest);

  int compare(T o1, T o2);

  int spaceNeededToEncode(Object obj);

  boolean isFixedSize();

  Class<?> getType();
}
