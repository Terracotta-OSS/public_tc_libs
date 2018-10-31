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
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.RecordData;
import org.terracotta.runnel.Struct;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

public class PredicatedDeleteRecordFullResponse<K extends Comparable<K>> extends DatasetEntityResponse {

  private final RecordData<K> deleted;

  public PredicatedDeleteRecordFullResponse(K key, long msn, Iterable<Cell<?>> cells) {
    this(new RecordData<>(msn, requireNonNull(key), requireNonNull(cells)));
  }

  public PredicatedDeleteRecordFullResponse() {
    this(null);
  }

  private PredicatedDeleteRecordFullResponse(RecordData<K> deleted) {
    this.deleted = deleted;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.PREDICATED_DELETE_RECORD_FULL_RESPONSE;
  }

  public RecordData<K> getDeleted() {
    return deleted;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.record("deleted", 20).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    PredicatedDeleteRecordFullResponse<?> deleteResponse = (PredicatedDeleteRecordFullResponse) response;
    ofNullable(deleteResponse.getDeleted()).ifPresent(r -> encoder.record("deleted", r));
  }

  public static DatasetEntityResponse decode(DatasetStructDecoder decoder) {
    RecordData<?> deleted = decoder.record("deleted");
    return new PredicatedDeleteRecordFullResponse<>(deleted);
  }
}
