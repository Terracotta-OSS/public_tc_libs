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
 * A checked exception that indicates a store operation has failed.
 */
public class StoreException extends Exception {
  private static final long serialVersionUID = -3095810976805646635L;

  /**
   * Create a StoreException with an underlying cause.
   *
   * @param cause the exception that led to this exception being created
   */
  public StoreException(Throwable cause) {
    super(cause.getMessage(), cause);
  }

  /**
   * Create a StoreException with a message.
   *
   * @param message the message
   */
  public StoreException(String message) {
    super(message);
  }

  /**
   * Create a StoreException with a message and underlying cause.
   *
   * @param message the message
   * @param cause the exception that led to this exception being created
   */
  public StoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
