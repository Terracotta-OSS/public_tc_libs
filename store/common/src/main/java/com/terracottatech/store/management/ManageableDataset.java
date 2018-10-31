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
package com.terracottatech.store.management;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.internal.InternalDataset;
import com.terracottatech.store.statistics.DatasetStatistics;
import com.terracottatech.store.statistics.StatisticsDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;

import java.util.Objects;
import java.util.function.Consumer;

import static com.terracottatech.store.management.ManageableDatasetManager.KEY_DM_NAME;
import static com.terracottatech.store.management.ManageableDatasetManager.KEY_D_INST;
import static com.terracottatech.store.management.ManageableDatasetManager.KEY_D_NAME;

@RequiredContext({@Named(KEY_DM_NAME), @Named(KEY_D_NAME), @Named(KEY_D_INST)})
class ManageableDataset<K extends Comparable<K>> implements InternalDataset<K> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManageableDataset.class);

  private final StatisticsDataset<K> underlying;
  private final Consumer<ManageableDataset<K>> onClose;
  private final Context context;

  ManageableDataset(Context parentContext, StatisticsDataset<K> underlying, Consumer<ManageableDataset<K>> onClose) {
    this.underlying = Objects.requireNonNull(underlying);
    this.onClose = Objects.requireNonNull(onClose);
    this.context = parentContext
        .with(ManageableDatasetManager.KEY_D_NAME, underlying.getStatistics().getDatasetName())
        .with(ManageableDatasetManager.KEY_D_INST, underlying.getStatistics().getInstanceName());
  }

  @Override
  public DatasetReader<K> reader() {
    return underlying.reader();
  }

  @Override
  public DatasetWriterReader<K> writerReader() {
    return underlying.writerReader();
  }

  @Override
  public Indexing getIndexing() {
    return underlying.getIndexing();
  }

  @Override
  public DatasetStatistics getStatistics() {return underlying.getStatistics();}

  @Override
  public Dataset<K> getUnderlying() {
    return underlying.getUnderlying();
  }


  @Override
  public void close() {
    try {
      onClose.accept(this);
    } catch (RuntimeException e) {
      LOGGER.trace("Error caught while closing: {}", e.getMessage(), e);
    } finally {
      underlying.close();
    }
  }

  public Context getContext() {
    return context;
  }
}
