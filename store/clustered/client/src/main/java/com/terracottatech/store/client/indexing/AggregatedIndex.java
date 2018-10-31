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
package com.terracottatech.store.client.indexing;

import com.terracottatech.store.client.DatasetEntity;
import com.terracottatech.store.common.indexing.ImmutableIndex;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.IndexSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

/**
 * A view of an multi-stripe index.  This index may be "live".
 *
 * @see LiveIndex
 */
public class AggregatedIndex<T extends Comparable<T>> implements Index<T> {

  private final CellDefinition<T> on;
  private final IndexSettings definition;
  private final Collection<Index<T>> indexes;
  private final Collection<DatasetEntity<?>> nonReportingEntities;

  /**
   * Constructs a new {@code AggregateIndex}.
   * @param on the {@code CellDefinition} on which the index is defined
   * @param definition the type of index
   * @param indexes the initial collection of <i>known</i> indexes
   * @param nonReportingEntities the {@code DatasetEntity} for all datasets than did <b>not</b> report on the index
   * @param <K> the key type for the dataset
   */
  <K extends Comparable<K>> AggregatedIndex(
      CellDefinition<T> on, IndexSettings definition, Collection<Index<T>> indexes, Collection<DatasetEntity<K>> nonReportingEntities) {
    this.on = on;
    this.definition = definition;
    this.indexes = new ArrayList<>(indexes);
    this.nonReportingEntities = new ArrayList<>(nonReportingEntities);
  }

  @Override
  public CellDefinition<T> on() {
    return on;
  }

  @Override
  public IndexSettings definition() {
    return definition;
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized Status status() {
    /*
     * The status of the aggregate is the "lowest" status among the stripe servers:
     *   a) if all servers report back on the index, the status is the lowest of those reported
     *   b) if some servers report back and some do not, the status is either "BROKEN" or "DEAD" --
     *      if one or more servers reports something other than "DEAD", the aggregate status is
     *      "BROKEN"; if all reporting servers report "DEAD", the aggregate status is "DEAD"
     * If one or more servers failed to report status and some did, then, because this is a "live"
     * status, the failed-to-report servers must be asked again -- their status may have changed.
     */
    Collection<Index<T>> allIndexes;
    if (!nonReportingEntities.isEmpty()) {
      allIndexes = new ArrayList<>();
      for (Iterator<DatasetEntity<?>> iterator = nonReportingEntities.iterator(); iterator.hasNext(); ) {
        DatasetEntity<?> entity = iterator.next();
        Optional<Index<?>> index = entity.getIndexing().getAllIndexes().stream()
            .filter(i -> i.on().equals(on()) && i.definition().equals(definition()))
            .findAny();
        if (index.isPresent()) {
          /*
           * The server knows of this index (again); add it to the known indexes and remove the
           * server from the non-reporting list.
           */
          indexes.add((Index<T>)index.get());   // unchecked
          iterator.remove();
        } else {
          /*
           * The server still knows nothing of this index; report it as broken (potentially undefined).
           */
          allIndexes.add(new ImmutableIndex<>(on(), definition(), Status.BROKEN));
        }
      }
      allIndexes.addAll(indexes);

    } else {
      allIndexes = indexes;
    }

    boolean allDeadOrBroken = true;
    Status aggregateStatus = Status.LIVE;
    for (Index<T> index : allIndexes) {
      Status status = index.status();
      allDeadOrBroken &= (Status.DEAD.compareTo(status) >= 0);
      aggregateStatus = (aggregateStatus.compareTo(status) > 0 ? status : aggregateStatus);
    }

    return allDeadOrBroken ? Status.DEAD : aggregateStatus;
  }
}
