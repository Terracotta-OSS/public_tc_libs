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

package com.terracottatech.store.common.messages.event;

import com.terracottatech.store.ChangeType;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;

public class ChangeEventResponse<K extends Comparable<K>> extends DatasetEntityResponse {
  private final K key;
  private final ChangeType changeType;

  public ChangeEventResponse(K key, ChangeType changeType) {
    this.key = key;
    this.changeType = changeType;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.CHANGE_EVENT_RESPONSE;
  }

  public K getKey() {
    return key;
  }

  public ChangeType getChangeType() {
    return changeType;
  }

  private static final EnumMapping<ChangeType> CHANGE_TYPE_ENUM_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(ChangeType.class)
          .mapping(ChangeType.ADDITION, 0)
          .mapping(ChangeType.MUTATION, 1)
          .mapping(ChangeType.DELETION, 2)
          .build();

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.key("key", 10)
            .enm("changeType", 20, CHANGE_TYPE_ENUM_MAPPING)
            .build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    ChangeEventResponse<?> changeResponse = (ChangeEventResponse<?>) response;
    encoder.key("key", changeResponse.getKey())
            .enm("changeType", changeResponse.getChangeType());
  }

  public static <K extends Comparable<K>> DatasetEntityResponse decode(DatasetStructDecoder decoder) {
    K key = decoder.key("key");
    ChangeType type = decoder.<ChangeType>enm("changeType").get();
    return new ChangeEventResponse<>(key, type);
  }
}
