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

import com.terracottatech.store.Cell;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import org.terracotta.runnel.Struct;

import java.util.Collection;
import java.util.UUID;

public class AddRecordMessage<K extends Comparable<K>> extends MutationMessage<K> {
  private final Iterable<Cell<?>> cells;

  public AddRecordMessage(UUID stableClientId, K key, Iterable<Cell<?>> cells, boolean respondInFull) {
    super(stableClientId, key, ChangeType.ADDITION, respondInFull);
    this.cells = cells;
  }

  public Iterable<Cell<?>> getCells() {
    return cells;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.ADD_RECORD_MESSAGE;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
        .uuid("stableClientId", 10)
        .key("key", 20)
        .bool("respondInFull", 30)
        .cells("cells", 50)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    AddRecordMessage<?> addMessage = (AddRecordMessage<?>) message;
    encoder
        .uuid("stableClientId", addMessage.getStableClientId())
        .key("key", addMessage.getKey())
        .bool("respondInFull", addMessage.isRespondInFull())
        .cells("cells", addMessage.getCells());
  }

  public static <K extends Comparable<K>> AddRecordMessage<K> decode(DatasetStructDecoder decoder) {
    UUID stableClientId = decoder.uuid("stableClientId");
    K key = decoder.key("key");
    Boolean respondInFull = decoder.bool("respondInFull");
    Collection<Cell<?>> cells = decoder.cells("cells");
    return new AddRecordMessage<>(stableClientId, key, cells, respondInFull);
  }
}
