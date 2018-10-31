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

public class PredicatedUpdateRecordFullResponse<K extends Comparable<K>> extends DatasetEntityResponse {

  private final RecordData<K> before;
  private final RecordData<K> after;

  public PredicatedUpdateRecordFullResponse(K key, long beforeMsn, Iterable<Cell<?>> beforeCells, long afterMsn, Iterable<Cell<?>> afterCells) {
    this(new RecordData<>(beforeMsn, requireNonNull(key), requireNonNull(beforeCells)), new RecordData<>(afterMsn, requireNonNull(key), requireNonNull(afterCells)));
  }

  public PredicatedUpdateRecordFullResponse() {
    this(null, null);
  }

  private PredicatedUpdateRecordFullResponse(RecordData<K> before, RecordData<K> after) {
    this.before = before;
    this.after = after;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.PREDICATED_UPDATE_RECORD_FULL_RESPONSE;
  }

  public RecordData<K> getBefore() {
    return before;
  }

  public RecordData<K> getAfter() {
    return after;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.record("before", 20).record("after", 30).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse message) {
    PredicatedUpdateRecordFullResponse<?> updateResponse = (PredicatedUpdateRecordFullResponse) message;
    ofNullable(updateResponse.getBefore()).ifPresent(r -> encoder.record("before", r));
    ofNullable(updateResponse.getAfter()).ifPresent(r -> encoder.record("after", r));
  }

  public static <K extends Comparable<K>> DatasetEntityResponse decode(DatasetStructDecoder decoder) {
    RecordData<K> before = decoder.record("before");
    RecordData<K> after = decoder.record("after");
    return new PredicatedUpdateRecordFullResponse<>(before, after);
  }
}
