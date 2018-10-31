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
 * Thrown when the requested dataset does not have the expected key type.
 */
public class DatasetKeyTypeMismatchException extends StoreException {
  private static final long serialVersionUID = 4946251850128496418L;

  public DatasetKeyTypeMismatchException(Throwable cause) {
    super(cause);
  }

  public DatasetKeyTypeMismatchException(String message) {
    super(message);
  }

  public DatasetKeyTypeMismatchException(String message, Throwable cause) {
    super(message, cause);
  }
}
