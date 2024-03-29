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

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetOperationMessageType;
import com.terracottatech.store.common.messages.DatasetStructBuilder;
import com.terracottatech.store.common.messages.DatasetStructDecoder;
import com.terracottatech.store.common.messages.DatasetStructEncoder;
import com.terracottatech.store.common.messages.ManagementMessage;
import org.terracotta.runnel.Struct;

public class SendChangeEventsMessage extends ManagementMessage {
  private final boolean sendChangeEvents;

  public SendChangeEventsMessage(boolean sendChangeEvents) {
    this.sendChangeEvents = sendChangeEvents;
  }

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.SEND_CHANGE_EVENTS_MESSAGE;
  }

  public boolean sendChangeEvents() {
    return sendChangeEvents;
  }

  public static Struct struct(DatasetStructBuilder builder) {
    return builder.bool("sendChangeEvents", 10).build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
    encoder.bool("sendChangeEvents", ((SendChangeEventsMessage) message).sendChangeEvents());
  }

  public static SendChangeEventsMessage decode(DatasetStructDecoder decoder) {
    return new SendChangeEventsMessage(decoder.bool("sendChangeEvents"));
  }

}
