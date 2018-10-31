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
package com.terracottatech.sovereign.indexing;

import com.terracottatech.store.StoreIndexNotFoundException;
import com.terracottatech.store.definition.CellDefinition;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Class for manipulating secondary indexes in Sovereign.
 *
 * Created by cschanck
 */
public interface SovereignIndexing {

  /**
   * Create a task that creates an index.
   * @param cellDefinition cell to index.
   * @param settings type of index.
   * @param <T> type
   * @return
   * @throws IllegalArgumentException
   */
  <T extends Comparable<T>> Callable<SovereignIndex<T>> createIndex(CellDefinition<T> cellDefinition,
          SovereignIndexSettings settings) throws IllegalArgumentException;

  <T extends Comparable<T>> void destroyIndex(SovereignIndex<T> index) throws StoreIndexNotFoundException;

  List<SovereignIndex<?>> getIndexes();

  <T extends Comparable<T>> SovereignIndex<T> getIndex(CellDefinition<T> cellDefinition, SovereignIndexSettings settings);
}
