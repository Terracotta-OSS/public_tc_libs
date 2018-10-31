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

import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import org.terracotta.runnel.Struct;

public class PredicatedDeleteRecordSimplifiedResponse extends DatasetEntityResponse {

  private final boolean deleted;

  public PredicatedDeleteRecordSimplifiedResponse(boolean deleted) {
    this.deleted = deleted;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.PREDICATED_DELETE_RECORD_SIMPLIFIED_RESPONSE;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.bool("deleted", 20).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    PredicatedDeleteRecordSimplifiedResponse deleteResponse = (PredicatedDeleteRecordSimplifiedResponse) response;
    encoder.bool("deleted", deleteResponse.isDeleted());
  }

  public static DatasetEntityResponse decode(DatasetStructDecoder decoder) {
    boolean deleted = decoder.bool("deleted");
    return new PredicatedDeleteRecordSimplifiedResponse(deleted);
  }
}
