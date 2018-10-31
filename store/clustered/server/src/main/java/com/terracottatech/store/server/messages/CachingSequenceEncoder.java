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

package com.terracottatech.store.server.messages;

import com.terracottatech.sovereign.impl.utils.CachingSequence;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

public class CachingSequenceEncoder {

  private static final String CACHE_MANY_ID = "cacheMany";
  private static final String NEXT_CHUNK_ID = "nextChunk";
  public static final Struct CACHING_SEQUENCE_STRUCT = StructBuilder.newStructBuilder()
      .int64(NEXT_CHUNK_ID, 10)
      .int32(CACHE_MANY_ID, 20)
      .build();

  public static void encode(StructEncoder<?> encoder, CachingSequence cachingSequence) {
    encoder.int64(NEXT_CHUNK_ID, cachingSequence.getNextChunk())
        .int32(CACHE_MANY_ID, cachingSequence.getCacheMany());
  }

  public static CachingSequence decode(StructDecoder<?> structDecoder) {
    return new CachingSequence(structDecoder.int64(NEXT_CHUNK_ID), structDecoder.int32(CACHE_MANY_ID));
  }

}
