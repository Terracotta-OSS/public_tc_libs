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

import org.terracotta.runnel.Struct;

/**
 * A no-op message for the universal concurrency key.  An instance of this message
 * is used as the signal message for in the {@code IEntityMessenger.deferRetirement}
 * method.  Because of this usage case, this message is <b>not</b> a singleton.
 */
public class UniversalNopMessage extends UniversalMessage {

  @Override
  public DatasetOperationMessageType getType() {
    return DatasetOperationMessageType.UNIVERSAL_NOP_MESSAGE;
  }

  public static Struct struct(DatasetStructBuilder datasetStructBuilder) {
    return datasetStructBuilder.build();
  }

  public static void encode(DatasetStructEncoder encoder, DatasetEntityMessage message) {
  }

  public static UniversalNopMessage decode(DatasetStructDecoder decoder) {
    return new UniversalNopMessage();
  }
}
