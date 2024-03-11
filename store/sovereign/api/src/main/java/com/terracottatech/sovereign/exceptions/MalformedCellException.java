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
package com.terracottatech.sovereign.exceptions;

/**
 * Indicates that a {@link com.terracottatech.store.Cell Cell} presented to a
 * {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset} method
 * that adds or changes a {@link com.terracottatech.store.Record Record} is
 * malformed.
 *
 * @author Clifford W. Johnson
 */
public class MalformedCellException extends SovereignRuntimeException {
  private static final long serialVersionUID = -7234304835607826590L;

  public MalformedCellException(final String message) {
    super(message);
  }
}
