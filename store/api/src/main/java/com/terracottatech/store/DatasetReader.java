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

import com.terracottatech.store.async.AsyncDatasetReader;
import com.terracottatech.store.stream.RecordStream;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Accessor that provides read only access on to a dataset.
 *
 * @param <K> the key type for the record
 */
public interface DatasetReader<K extends Comparable<K>> {

  /**
   * Return the key type of underlying {@code Dataset}.
   *
   * @return the key type
   */
  Type<K> getKeyType();

  /**
   * Retrieves the record for the specified key. If this dataset contains a
   * record for the supplied key, then an Optional containing that record is returned.  If there is no
   * record for this key, then an empty Optional is returned.
   *
   * @param key key for the record
   * @return an Optional containing the record for the given key, or an empty Optional if there is no record held
   * against the key.
   */
  Optional<Record<K>> get(K key);

  /**
   * Returns a ReadRecordAccessor which can be used to for fine-control of
   * read operations on the record held against the specified key.
   *
   * @param key key for the record
   * @return a ReadRecordAccessor tied to the supplied key.
   */
  ReadRecordAccessor<K> on(K key);

  /**
   * Returns a {@link java.util.stream.Stream} of the records in this dataset.
   *
   * @return a stream of records
   */
  RecordStream<K> records();

  /**
   * Returns an asynchronous version of this reader.
   *
   * @return an asynchronous dataset reader
   */
  AsyncDatasetReader<K> async();

  /**
   * Registers a listener that will receive an event for changes to a key.
   *
   * Changes that trigger events are:
   * the addition of a key, and associated record;
   * the mutation of a record;
   * the deletion of a key, and associated record.
   * <p>
   * The order of events that would be received for changes to a particular
   * key will be same as the order in which the server has made those changes on
   * that key. Note: This ordering of events is guaranteed only for changes to a
   * key and not across the keys.
   * </p>
   *
   * @param listener the user provided object to which events will be sent
   */
  void registerChangeListener(ChangeListener<K> listener);

  /**
   * Deregisters a listener so that it no longer receives events
   *
   * @param listener the listener to be deregistered
   */
  void deregisterChangeListener(ChangeListener<K> listener);
}
