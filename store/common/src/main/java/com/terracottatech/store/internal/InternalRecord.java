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
package com.terracottatech.store.internal;

import com.terracottatech.store.Record;

public interface InternalRecord<K extends Comparable<K>> extends Record<K> {

  /**
   * Gets the Mutation Sequence Number (MSN) assigned to this record.  This number is
   * strictly increasing for the key in this record, and has no relationship to MSNs
   * for other keys. For a versioned record, the MSN from the most recent version is returned.
   *
   * @return the mutation sequence number for this {@code {@link InternalRecord}
   */
  long getMSN();

}
