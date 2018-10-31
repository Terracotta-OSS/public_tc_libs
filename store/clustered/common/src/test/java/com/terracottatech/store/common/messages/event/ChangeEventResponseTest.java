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
import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChangeEventResponseTest {
  @Test
  public void getValuesPassedToConstructor() {
    ChangeEventResponse<String> response = new ChangeEventResponse<>("key", ChangeType.ADDITION);
    Assert.assertEquals(DatasetEntityResponseType.CHANGE_EVENT_RESPONSE, response.getType());
    assertEquals("key", response.getKey());
    assertEquals(ChangeType.ADDITION, response.getChangeType());
  }
}
