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
package com.terracottatech.store.client.indexing;

import com.terracottatech.store.client.message.MessageSender;
import com.terracottatech.store.client.message.SendConfiguration;
import com.terracottatech.store.common.messages.indexing.IndexStatusMessage;
import com.terracottatech.store.common.messages.indexing.IndexStatusResponse;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

/**
 * A non-static ("live") view of a remote index.
 */
public class LiveIndex<T extends Comparable<T>> implements Index<T> {

  private final MessageSender messageSender;
  private final CellDefinition<T> on;
  private final IndexSettings definition;

  LiveIndex(MessageSender messageSender, CellDefinition<T> on, IndexSettings definition) {
    this.messageSender = messageSender;
    this.on = on;
    this.definition = definition;
  }

  @Override
  public CellDefinition<T> on() {
    return on;
  }

  @Override
  public IndexSettings definition() {
    return definition;
  }

  /**
   * Gets the current status from the remote index.
   * @return the current status
   */
  @Override
  public Status status() {
    IndexStatusResponse statusResponse = messageSender.sendMessageAwaitResponse(
        new IndexStatusMessage<>(on, definition), SendConfiguration.ONE_SERVER);
    return statusResponse.getStatus();
  }
}
