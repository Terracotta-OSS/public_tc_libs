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
 *  Indicates an exception that is a normal server response to a client asking for the wrong thing
 *  as part of a normal flow in the API. One example might be a client asking for an index to be
 *  created when it already exists. Such an situation will not require to be logged as an error on
 *  the server.
 *  If this exception is thrown on the passive, it will not cause the passive to crash.
 */
@SuppressWarnings("serial")
public class ClientSideOnlyException extends StoreRuntimeException {
  public ClientSideOnlyException(Throwable cause) {
    super(cause);
  }
}
