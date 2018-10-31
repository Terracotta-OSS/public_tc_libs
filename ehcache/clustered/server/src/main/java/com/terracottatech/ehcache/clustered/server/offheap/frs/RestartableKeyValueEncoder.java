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
package com.terracottatech.ehcache.clustered.server.offheap.frs;

import java.nio.ByteBuffer;

final class RestartableKeyValueEncoder {
  private static final int KEY_HASH_CODE_OFFSET = 0;
  private static final int KEY_ENCODING_SIZE = 4;

  private static final int VALUE_ENCODING_OFFSET = 0;
  private static final int VALUE_METADATA_OFFSET = 8;
  private static final int VALUE_ENCODING_SIZE = 12;

  private RestartableKeyValueEncoder() {
  }

  public static ByteBuffer encodeKey(ByteBuffer keyBuffer, int hash) {
    //encode hashcode in first 4 bytes of key form
    ByteBuffer frsKeyBuffer = ByteBuffer.allocate(keyBuffer.remaining() + KEY_ENCODING_SIZE);
    frsKeyBuffer.putInt(hash).put(keyBuffer).flip();
    return frsKeyBuffer;
  }

  public static int extractHashCodeFromKeyBuffer(ByteBuffer frsBinaryKey) {
    return frsBinaryKey.getInt(KEY_HASH_CODE_OFFSET);
  }

  @SuppressWarnings({ "cast", "RedundantCast" })
  public static ByteBuffer extractKeyFromKeyBuffer(ByteBuffer frsKeyBuffer) {
    int origPos = frsKeyBuffer.position();
    try {
      return ((ByteBuffer) frsKeyBuffer.position(origPos + KEY_ENCODING_SIZE)).slice();       // made redundant in Java 9/10
    } finally {
      frsKeyBuffer.position(origPos);
    }
  }

  public static ByteBuffer encodeValue(ByteBuffer offheapBinaryValue, long encoding, int metadata) {
    //encode encoding in first 8 bytes of key form
    ByteBuffer frsBinaryValue = ByteBuffer.allocate(offheapBinaryValue.remaining() + VALUE_ENCODING_SIZE);
    frsBinaryValue.putLong(encoding).putInt(metadata).put(offheapBinaryValue).flip();
    return frsBinaryValue;
  }

  @SuppressWarnings({ "cast", "RedundantCast" })
  public static ByteBuffer extractValueFromValueBuffer(ByteBuffer frsBinaryValue) {
    int origPos = frsBinaryValue.position();
    try {
      return ((ByteBuffer) frsBinaryValue.position(origPos + VALUE_ENCODING_SIZE)).slice();       // made redundant in Java 9/10
    } finally {
      frsBinaryValue.position(origPos);
    }
  }

  public static ByteBuffer duplicatedByteBuffer(ByteBuffer buf) {
    return duplicatedByteBuffer(buf, buf.isDirect());
  }

  @SuppressWarnings({ "cast", "RedundantCast" })
  public static ByteBuffer duplicatedByteBuffer(ByteBuffer buf, boolean direct) {
    ByteBuffer src = buf.duplicate();
    ByteBuffer dest = direct ? ByteBuffer.allocateDirect(src.remaining()) :
        ByteBuffer.allocate(src.remaining());
    dest.put(src);
    return (ByteBuffer)dest.flip();       // made redundant in Java 9/10
  }

  public static long extractEncodingFromValueBuffer(ByteBuffer frsBinaryValue) {
    return frsBinaryValue.getLong(VALUE_ENCODING_OFFSET);
  }

  public static int extractMetadataFromValueBuffer(ByteBuffer frsBinaryValue) {
    return frsBinaryValue.getInt(VALUE_METADATA_OFFSET);
  }
}