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
package com.terracottatech.store;

/**
 * Throw to indicate an operation is abandoned due to a connection failure.  The completion
 * status of the abandoned operation is unknown.  The value of {@link #getCause()} is the exception by which the
 * connection failure was observed.
 */
public class StoreOperationAbandonedException extends StoreRuntimeException {
  private static final long serialVersionUID = 3386356546990873530L;

  public StoreOperationAbandonedException(String message) {
    super(message);
  }

  public StoreOperationAbandonedException(Throwable cause) {
    super(cause);
  }

  public StoreOperationAbandonedException(String message, Throwable cause) {
    super(message, cause);
  }
}
