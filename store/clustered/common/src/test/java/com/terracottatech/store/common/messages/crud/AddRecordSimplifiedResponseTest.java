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

import com.terracottatech.store.common.messages.DatasetEntityResponseType;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AddRecordSimplifiedResponseTest {
  @Test
  public void givesAddRecordType() {
    AddRecordSimplifiedResponse response = new AddRecordSimplifiedResponse(true);
    Assert.assertEquals(DatasetEntityResponseType.ADD_RECORD_SIMPLIFIED_RESPONSE, response.getType());
    assertTrue(response.isAdded());
  }

  @Test
  public void givesAlreadyExistsResult() {
    AddRecordSimplifiedResponse response = new AddRecordSimplifiedResponse(false);
    assertFalse(response.isAdded());
  }
}
