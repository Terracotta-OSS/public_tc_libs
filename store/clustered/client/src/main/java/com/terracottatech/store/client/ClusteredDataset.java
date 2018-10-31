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
package com.terracottatech.store.client;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.indexing.Indexing;

/**
 * An implementation of Dataset that uses a DatasetEntity to meet the API - essentially an adapter between the Voltron
 * API and the store Dataset interface.
 *
 * @param <K> the key type
 */
public class ClusteredDataset<K extends Comparable<K>> implements Dataset<K> {
  private final DatasetEntity<K> entity;

  /**
   * Creates a new ClusteredDataset that wraps the DatasetEntity (indirectly the Voltron connection).
   *
   * @param entity the DatasetEntity - ownership is transferred to the constructed object
   */
  public ClusteredDataset(DatasetEntity<K> entity) {
    this.entity = entity;
  }

  @Override
  public DatasetReader<K> reader() {
    return new ClusteredDatasetReader<>(entity);
  }

  @Override
  public DatasetWriterReader<K> writerReader() {
    return new ClusteredDatasetWriterReader<>(entity);
  }

  @Override
  public Indexing getIndexing() {
    return entity.getIndexing();
  }

  @Override
  public void close() {
    entity.close();
  }
}
