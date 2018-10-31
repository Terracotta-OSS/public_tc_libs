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
package com.terracottatech.store.client.message;

import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import org.terracotta.entity.InvocationBuilder;

public interface SendConfiguration {
  SendConfiguration FULL = builder -> {};
  SendConfiguration GUARANTEED_SEND = builder -> builder.replicate(true).ackSent();
  SendConfiguration WITHOUT_REPLICATION = builder -> builder.replicate(false).ackRetired();
  SendConfiguration ONE_SERVER = InvocationBuilder::ackReceived;
  SendConfiguration FIRE_AND_FORGET = builder -> builder.replicate(false).blockGetOnRetire(false);

  void configure(InvocationBuilder<DatasetEntityMessage, DatasetEntityResponse> invocationBuilder);
}
