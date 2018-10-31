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

package com.terracottatech.store.common.messages.crud;

import com.terracottatech.store.Record;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.UniversalMessage;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import org.terracotta.runnel.Struct;

import static java.util.Objects.requireNonNull;

public class GetRecordMessage<K extends Comparable<K>> extends UniversalMessage {
  private final K key;
  private final IntrinsicPredicate<? super Record<K>> predicate;

  public GetRecordMessage(K key, IntrinsicPredicate<? super Record<K>> predicate) {
    this.key = requireNonNull(key);
    this.predicate = requireNonNull(predicate);
  }

  public K getKey() {
    return key;
  }

  public IntrinsicPredicate<? super Record<K>> getPredicate() {
    return predicate;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.GET_RECORD_MESSAGE;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
            .key("key", 10)
            .intrinsic("predicate", 20)
            .build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    GetRecordMessage<?> getMessage = (GetRecordMessage<?>) message;
    encoder.key("key", getMessage.getKey()).intrinsic("predicate", getMessage.getPredicate());
  }

  public static <K extends Comparable<K>> GetRecordMessage<K> decode(DatasetStructDecoder decoder) {
    K key = decoder.key("key");
    @SuppressWarnings("unchecked")
    IntrinsicPredicate<? super Record<K>> predicate = (IntrinsicPredicate<? super Record<K>>) decoder.intrinsic("predicate");
    return new GetRecordMessage<>(key, predicate);
  }
}
