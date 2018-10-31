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
package com.terracottatech.ehcache.common.frs;

import com.tc.classloader.CommonComponent;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RestartableMap;

import java.nio.ByteBuffer;

/**
 * Restartable maps for any generic type.
 * <p>
 *   Makes heavy use of codecs to allow users to create maps from different types for key and value types
 *
 * @param <K> key type
 * @param <V> value type
 *
 * @author RKAV
 */
@CommonComponent
public class RestartableGenericMap<K, V> extends RestartableMap<K, V, ByteBuffer, ByteBuffer, ByteBuffer> {
  private final FrsCodec<K> keyCodec;
  private final FrsCodec<V> valueCodec;

  public RestartableGenericMap(
      Class<K> keyType,
      Class<V> valueType,
      ByteBuffer identifier,
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore) {
    this(keyType, valueType, identifier, restartStore, true);
  }

  public RestartableGenericMap(
      Class<K> keyType,
      Class<V> valueType,
      ByteBuffer identifier,
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore, boolean synchronousWrites) {
    super(identifier, restartStore, synchronousWrites);
    keyCodec = FrsCodecFactory.getCodec(keyType);
    valueCodec = FrsCodecFactory.getCodec(valueType);
  }
  @Override
  protected ByteBuffer encodeKey(K k) {
    return keyCodec.encode(k);
  }

  @Override
  protected ByteBuffer encodeValue(V v) {
    return valueCodec.encode(v);
  }

  @Override
  protected K decodeKey(ByteBuffer byteBuffer) {
    return keyCodec.decode(byteBuffer);
  }

  @Override
  protected V decodeValue(ByteBuffer byteBuffer) {
    return valueCodec.decode(byteBuffer);
  }

  @Override
  protected long keyByteSize(K k, ByteBuffer encodedKey) {
    return encodedKey.capacity();
  }

  @Override
  protected long valueByteSize(V v, ByteBuffer encodedValue) {
    return encodedValue.capacity();
  }
}