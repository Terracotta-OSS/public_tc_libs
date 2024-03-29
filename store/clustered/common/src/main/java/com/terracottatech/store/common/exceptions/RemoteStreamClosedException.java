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
package com.terracottatech.store.common.exceptions;

import com.terracottatech.store.StoreRuntimeException;

/**
 * Thrown to indicate the remote stream underlying an operation is closed.
 * This is different from a Java stream which throws an {@link IllegalStateException} with the message
 * {@code stream has already been operated upon or closed}.
 */
public class RemoteStreamClosedException extends StoreRuntimeException {
  private static final long serialVersionUID = 7723766018978682253L;

  public RemoteStreamClosedException(Throwable cause) {
    super(cause);
  }

  public RemoteStreamClosedException(String message) {
    super(message);
  }

  public RemoteStreamClosedException(String message, Throwable cause) {
    super(message, cause);
  }
}
