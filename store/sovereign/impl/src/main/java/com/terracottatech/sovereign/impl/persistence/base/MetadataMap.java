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
package com.terracottatech.sovereign.impl.persistence.base;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RestartableMap;
import com.terracottatech.sovereign.common.utils.NIOBufferUtils;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
class MetadataMap extends RestartableMap<MetadataKey<?>, Object, ByteBuffer, ByteBuffer, ByteBuffer> {
  private AbstractRestartabilityBasedStorage storage;

  public MetadataMap(AbstractRestartabilityBasedStorage storage, ByteBuffer identifier,
                     RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability) {
    super(identifier, restartability, true);
    this.storage = storage;
  }

  @Override
  protected void replayPut(MetadataKey<?> key, Object value) {
    if (key.getTag() == MetadataKey.Tag.DATASET_DESCR) {
      // hook in the storage if this is the dataset.
      SovereignDatasetDescriptionImpl<?, ?> impl = (SovereignDatasetDescriptionImpl<?, ?>) value;
      impl.getConfig().storage(storage);
    }
    super.replayPut(key, value);
  }

  @Override
  protected ByteBuffer encodeKey(MetadataKey<?> key) {
    return key.toBuffer();
  }

  @Override
  protected ByteBuffer encodeValue(Object object) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos;
    try {
      oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.flush();
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected MetadataKey<?> decodeKey(ByteBuffer buffer) {
    return new MetadataKey<>(buffer);
  }

  @Override
  protected Object decodeValue(ByteBuffer buffer) {
    ByteArrayInputStream bais = new ByteArrayInputStream(NIOBufferUtils.dup(buffer, true).array());
    try {
      ObjectInputStream ois = new ObjectInputStream(bais);
      Object obj = ois.readObject();
      ois.close();
      return obj;
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected long keyByteSize(MetadataKey<?> key, ByteBuffer buffer) {
    return MetadataKey.byteFootprint();
  }

  @Override
  protected long valueByteSize(Object object, ByteBuffer buffer) {
    return buffer.remaining();
  }

  @Override
  public Object remove(Object key) {
    return super.remove(key);
  }
}
