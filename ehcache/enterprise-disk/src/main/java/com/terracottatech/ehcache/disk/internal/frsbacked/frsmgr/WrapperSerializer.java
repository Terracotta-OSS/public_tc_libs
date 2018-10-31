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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.nio.ByteBuffer;

/**
 * Wrapper for late binding of the actual serializer, when a store is attached to a
 * recovered map.
 *
 * @author RKAV
 */
final class WrapperSerializer<T> implements Serializer<T> {
  private volatile Serializer<T> delegate;

  WrapperSerializer() {
  }

  void changeSerializer(Serializer<T> serializer) {
    this.delegate = serializer;
  }

  @Override
  public ByteBuffer serialize(T object) throws SerializerException {
    if (delegate != null) {
      return delegate.serialize(object);
    } else {
      return null;
    }
  }

  @Override
  public T read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    if (delegate != null) {
      return delegate.read(binary);
    }
    return null;
  }

  @Override
  public boolean equals(T object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
    return delegate != null && delegate.equals(object, binary);
  }
}