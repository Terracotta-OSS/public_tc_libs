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

import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.common.messages.ErrorResponse;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.crud.GetRecordResponse;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ErrorResponseProcessorTest {
  @Test
  public void returnsMessageIfNotError() {
    ErrorResponseProcessor processor = new ErrorResponseProcessor();
    AddRecordSimplifiedResponse response = processor.process(new AddRecordSimplifiedResponse(true));
    assertNotNull(response);
  }

  @Test(expected = IllegalStateException.class)
  public void throwsRuntimeException() {
    ErrorResponseProcessor processor = new ErrorResponseProcessor();
    GetRecordResponse<Long> response = processor.process(new ErrorResponse(new IllegalStateException()));
    assertNotNull(response);
  }

  @Test(expected = OutOfMemoryError.class)
  public void throwsError() {
    ErrorResponseProcessor processor = new ErrorResponseProcessor();
    GetRecordResponse<Long> response = processor.process(new ErrorResponse(new OutOfMemoryError()));
    assertNotNull(response);
  }

  @Test(expected = StoreRuntimeException.class)
  public void throwsRuntimeExceptionForException() {
    ErrorResponseProcessor processor = new ErrorResponseProcessor();
    GetRecordResponse<Long> response = processor.process(new ErrorResponse(new Exception()));
    assertNotNull(response);
  }
}
