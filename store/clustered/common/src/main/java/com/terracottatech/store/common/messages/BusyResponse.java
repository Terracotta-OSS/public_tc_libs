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
package com.terracottatech.store.common.messages;

import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.EnumMappingBuilder;
import org.terracotta.runnel.Struct;


/**
 * Server-to-client response indicating an operation cannot be processed
 * because of a pending request.  The operation should be retried later.
 */
public class BusyResponse extends DatasetEntityResponse {

  private static final String REASON = "reason";

  public enum Reason {
    /**
     * The request cannot be completed because the specified key is under mutation.
     */
    KEY,
    /**
     * The request cannot be completed because the request cannot be enqueued in a background task.
     */
    EXEC
  }

  private final Reason reason;

  public BusyResponse(Reason reason) {
    this.reason = reason;
  }

  public Reason getReason() {
    return reason;
  }

  @Override
  public DatasetEntityResponseType getType() {
    return DatasetEntityResponseType.BUSY_RESPONSE;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("BusyResponse{");
    sb.append("reason=").append(reason);
    sb.append('}');
    return sb.toString();
  }

  private static final EnumMapping<Reason> REASON_MAPPING = EnumMappingBuilder.newEnumMappingBuilder(Reason.class)
      .mapping(Reason.KEY, 0)
      .mapping(Reason.EXEC,  1)
      .build();

  public static Struct struct(DatasetStructBuilder builder) {
    return builder
        .enm(REASON, 10, REASON_MAPPING)
        .build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityResponse response) {
    BusyResponse busyResponse = (BusyResponse)response;
    encoder
        .enm(REASON, busyResponse.getReason());
  }

  public static BusyResponse decode(DatasetStructDecoder decoder) {
    Reason reason = decoder.<Reason>enm(REASON).get();
    return new BusyResponse(reason);
  }
}
