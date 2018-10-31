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
package com.terracottatech.store.client;

import com.terracottatech.store.Cell;
import com.terracottatech.store.common.messages.StoreStructures;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicDescriptor;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicType;
import com.terracottatech.store.intrinsics.impl.RecordEquality;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

import static com.terracottatech.store.common.messages.StoreStructures.decodeCells;
import static com.terracottatech.store.common.messages.StoreStructures.decodeKey;
import static com.terracottatech.store.common.messages.StoreStructures.encodeKey;
import static com.terracottatech.store.intrinsics.IntrinsicType.PREDICATE_RECORD_EQUALS;

/**
 * Intrinsic descriptors overridden on the client-side.
 */
class ClientIntrinsicDescriptors {

  static final Map<IntrinsicType, IntrinsicDescriptor> OVERRIDDEN_DESCRIPTORS = new EnumMap<>(IntrinsicType.class);

  static {
    OVERRIDDEN_DESCRIPTORS.put(PREDICATE_RECORD_EQUALS, new IntrinsicDescriptor(
            IntrinsicCodec.INTRINSIC_DESCRIPTORS.get(PREDICATE_RECORD_EQUALS),
            (StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic, IntrinsicCodec intrinsicCodec) ->
                    encode(intrinsicEncoder, intrinsic),
            (StructDecoder<?> intrinsicDecoder, IntrinsicCodec intrinsicCodec) ->
                    decode(intrinsicDecoder))
    );
  }

  private static void encode(StructEncoder<?> intrinsicEncoder, Intrinsic intrinsic) {
    RecordEquality<?> recordEquality = (RecordEquality<?>) intrinsic;
    RecordImpl<?> record = (RecordImpl) recordEquality.getRecord();
    encodeKey(intrinsicEncoder.struct("key"), record.getKey());
    intrinsicEncoder.int64("msn", record.getMSN());
    intrinsicEncoder.structs("cells", record, StoreStructures::encodeCell);
  }

  private static <K extends Comparable<K>> Intrinsic decode(StructDecoder<?> intrinsicDecoder) {
    K key = decodeKey(intrinsicDecoder.struct("key"));
    Long msn = intrinsicDecoder.int64("msn");
    Collection<Cell<?>> cells = decodeCells(intrinsicDecoder.structs("cells"));
    RecordImpl<?> record = new RecordImpl<>(msn, key, cells);
    return new RecordEquality<>(record);
  }
}
