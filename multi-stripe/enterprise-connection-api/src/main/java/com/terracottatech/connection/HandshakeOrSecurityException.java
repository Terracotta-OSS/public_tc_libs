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
package com.terracottatech.connection;

/**
 * This exception is thrown when a client's handshake with server fails. This can happen when:
 * <ul>
 * <li>There's a mismatch between the security configuration of the client and the server</li>
 * <li>The client attempts to connect to the server when the server is about to shut down</li>
 * <li>The client attempts tothe server is stuck in a split-brain</li>
 * </ul>
 */
public class HandshakeOrSecurityException extends RuntimeException {
  private static final long serialVersionUID = 5753267498932817667L;

  public HandshakeOrSecurityException(String message, Throwable cause) {
    super(message, cause);
  }
}
