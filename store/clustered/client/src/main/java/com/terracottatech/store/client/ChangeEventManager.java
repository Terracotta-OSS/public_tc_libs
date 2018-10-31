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
package com.terracottatech.store.client;

import com.terracottatech.store.ChangeListener;
import com.terracottatech.store.ChangeType;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.event.ChangeEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeEventManager<K extends Comparable<K>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventManager.class);

  private final Type<K> keyType;
  private final ChangeListener<K> changeListener;

  public ChangeEventManager(Type<K> keyType, ChangeListener<K> changeListener) {
    this.keyType = keyType;
    this.changeListener = changeListener;
  }

  public void changeEvent(ChangeEventResponse<?> changeEvent) {
    Type<?> keyTypeReceived = Type.forJdkType(changeEvent.getKey().getClass());
    if (!keyType.equals(keyTypeReceived)) {
      LOGGER.error("Received wrong key type: " + keyTypeReceived);
      return;
    }
    @SuppressWarnings("unchecked")
    ChangeEventResponse<K> changeEventResponse = (ChangeEventResponse<K>) changeEvent;
    notifyChangeListener(changeEventResponse);
  }

  private void notifyChangeListener(ChangeEventResponse<K> changeEventResponse) {
    K key = changeEventResponse.getKey();
    ChangeType changeType = changeEventResponse.getChangeType();

    changeListener.onChange(key, changeType);
  }

  public void missedEvents() {
    changeListener.missedEvents();
  }
}
