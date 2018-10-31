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

package com.terracottatech.store.common.messages;

import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;

/**
 * Message payload that is {@link Record} data.
 * @param <K> the record key type
 */
public final class RecordData<K extends Comparable<K>> {
  private final long msn;
  private final K key;
  private final Iterable<Cell<?>> cells;

  public RecordData(long msn, K key, Iterable<Cell<?>> cells) {
    this.msn = msn;
    this.key = key;
    this.cells = cells;
  }

  /**
   * The <i>mutation sequence number</i> of the server-side version of the record encoded in this message.
   * @return the record mutation sequence number
   */
  public long getMsn() {
    return msn;
  }

  /**
   * Gets the key of the {@link Record} element emitted from the remote stream.
   *
   * @return the record key
   */
  public K getKey() {
    return key;
  }

  /**
   * Gets the cells of the {@link Record} element emitted from the remote stream.
   *
   * @return the cells
   */
  public Iterable<Cell<?>> getCells() {
    return cells;
  }
}
