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
import com.terracottatech.store.common.exceptions.ReflectiveExceptionBuilder;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.ErrorResponse;

public class ErrorResponseProcessor implements ResponseProcessor {
  @Override
  public <R extends DatasetEntityResponse> R process(DatasetEntityResponse response) {
    if (!(response instanceof ErrorResponse)) {
      @SuppressWarnings("unchecked")
      R errorResponse = (R) response;
      return errorResponse;
    }

    ErrorResponse errorResponse = (ErrorResponse) response;
    Throwable serverException = errorResponse.getCause();

    // convert the server-side stack trace to a client-side stack trace
    Class<? extends Throwable> exceptionClass = serverException.getClass();
    String exceptionMessage = serverException.getMessage();
    Throwable clientException = ReflectiveExceptionBuilder.buildThrowable(exceptionClass, exceptionMessage, serverException);

    if (clientException instanceof RuntimeException) {
      throw (RuntimeException) clientException;
    }

    if (clientException instanceof Error) {
      throw (Error) clientException;
    }

    if (serverException.getCause() instanceof OutOfMemoryError) {
      throw new StoreRuntimeException("Dataset operation failed due to server memory constraints. Please allocate more resources.", clientException);
    }

    throw new StoreRuntimeException(clientException);
  }
}
