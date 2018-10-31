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
package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public interface RecordBufferStrategy<K extends Comparable<K>>{

  /**
   * Encodes a {@code PersistentRecord} into a new {@code ByteBuffer}.
   *
   * @param record the {@code PersistentRecord} to encode
   *
   * @return the new {@code ByteBuffer} containing the encoded record; the buffer's position is set to the
   *      beginning of the record and the buffer's limit is set to the end of the record
   */
  ByteBuffer toByteBuffer(SovereignPersistentRecord<K> record);

  /**
   * Extracts a {@code PersistentRecord} from the current position of the {@code ByteBuffer} provided.
   * The buffer's current position is left unaltered.
   *
   * @param buf the {@code ByteBuffer} from which the record is extracted
   *
   * @return the {@code PersistentRecord} instance formed from {@code buf}
   */
  SovereignPersistentRecord<K> fromByteBuffer(ByteBuffer buf);

  /**
   * Extracts the key from the {@code PersistentRecord} encoded at the current position of the {@code ByteBuffer}
   * provided.  The buffer's current position is left unaltered.
   *
   * @param buf the {@code ByteBuffer} from which the record key is extracted
   *
   * @return the key from the {@code PersistentRecord} encoded in {@code buf}
   */
  K readKey(ByteBuffer buf);

  default boolean fromConsumesByteBuffer() {
    return false;
  }
}
