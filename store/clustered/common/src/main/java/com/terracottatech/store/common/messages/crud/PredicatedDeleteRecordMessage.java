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

import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Record;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import org.terracotta.runnel.Struct;

import java.util.UUID;

public class PredicatedDeleteRecordMessage<K extends Comparable<K>> extends MutationMessage<K> {
  private final IntrinsicPredicate<? super Record<K>> predicate;

  public PredicatedDeleteRecordMessage(UUID stableClientId, K key, IntrinsicPredicate<? super Record<K>> predicate, boolean respondInFull) {
    super(stableClientId, key, ChangeType.DELETION, respondInFull);
    this.predicate = predicate;
  }

  public IntrinsicPredicate<? super Record<K>> getPredicate() {
    return predicate;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.PREDICATED_DELETE_RECORD_MESSAGE;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
        .uuid("stableClientId", 10)
        .key("key", 20)
        .bool("respondInFull", 30)
        .intrinsic("predicate", 50)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    PredicatedDeleteRecordMessage<?> deleteMessage = (PredicatedDeleteRecordMessage<?>) message;
    encoder
        .uuid("stableClientId", deleteMessage.getStableClientId())
        .key("key", deleteMessage.getKey())
        .bool("respondInFull", deleteMessage.isRespondInFull())
        .intrinsic("predicate", deleteMessage.getPredicate());
  }

  public static <K extends Comparable<K>> PredicatedDeleteRecordMessage<K> decode(DatasetStructDecoder decoder) {
    UUID stableClientId = decoder.uuid("stableClientId");
    K key = decoder.key("key");
    boolean respondInFull = decoder.bool("respondInFull");
    @SuppressWarnings("unchecked")
    IntrinsicPredicate<? super Record<K>> predicate = (IntrinsicPredicate<? super Record<K>>) decoder.intrinsic("predicate");
    return new PredicatedDeleteRecordMessage<>(stableClientId, key, predicate, respondInFull);
  }
}
